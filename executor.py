"""数据处理层：异步任务表 + 工作线程池 + 结果文件落盘与回收。

并发模型（参数全部来自配置 mcp.db.async / mcp.db.result）：
  max-pool-size   工作线程数 = 同时在跑的查询上限
  queue-capacity  排队上限；满了 submit 抛 QueueFull（调用方转成“稍后重试”），
                  绝不让请求无限堆积
  ttl-minutes     任务在内存里的保留时长，查一次状态刷新一次“最近访问”
  max-tasks       任务表上限，超出按最久未访问淘汰（RUNNING 的不淘汰）
  result.*        结果文件 TTL 与目录总容量兜底，由后台清理线程周期回收

行数刻意不回传给模型（只进 QUERY_LOG 服务端日志）——该不该取明细应该在
写 SQL 时决定，模型一旦知道行数就会纠结要不要拉全量（Java 版同理）。
"""
from __future__ import annotations

import csv
import logging
import queue
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path

import database
from settings import InstanceSettings, Settings

QUERY_LOG = logging.getLogger("QUERY_LOG")
JANITOR_INTERVAL_SECONDS = 60


class TaskStore:
    """queryId → 任务快照。后台线程写、工具线程读，全部操作在锁内。

    快照字段：status(RUNNING/SUCCESS/FAILED) / time / file / rows / elapsed_ms / error
    """

    def __init__(self, max_tasks: int, ttl_seconds: float):
        self._tasks: OrderedDict[str, dict] = OrderedDict()
        self._lock = threading.Lock()
        self._max_tasks = max_tasks
        self._ttl_seconds = ttl_seconds

    def add_running(self, query_id: str) -> None:
        with self._lock:
            self._evict_locked()
            self._tasks[query_id] = {"status": "RUNNING", "time": time.time()}

    def finish(self, query_id: str, **fields: object) -> None:
        with self._lock:
            self._tasks[query_id] = {"status": fields.pop("status"), "time": time.time(), **fields}
            self._tasks.move_to_end(query_id)

    def drop(self, query_id: str) -> None:
        with self._lock:
            self._tasks.pop(query_id, None)

    def get(self, query_id: str) -> dict | None:
        """取任务快照（新 dict，不暴露内部引用）。过期即删并返回 None。"""
        with self._lock:
            task = self._tasks.get(query_id)
            if task is None:
                return None
            if task["status"] != "RUNNING" and time.time() - task["time"] > self._ttl_seconds:
                del self._tasks[query_id]
                return None
            self._tasks.move_to_end(query_id)   # 最近访问的活得久
            return dict(task)

    def purge_expired(self) -> int:
        """清掉所有过期任务，返回清理数量（清理线程周期调用）。"""
        now = time.time()
        with self._lock:
            expired = [k for k, t in self._tasks.items()
                       if t["status"] != "RUNNING" and now - t["time"] > self._ttl_seconds]
            for k in expired:
                del self._tasks[k]
            return len(expired)

    def _evict_locked(self) -> None:
        """超上限时按最久未访问淘汰（从 OrderedDict 头部即最旧端找非 RUNNING 的删）。"""
        while len(self._tasks) >= self._max_tasks:
            victim = next((k for k, t in self._tasks.items() if t["status"] != "RUNNING"), None)
            if victim is None:
                break   # 全在执行中：宁可不收新任务也不误删 RUNNING
            del self._tasks[victim]


class QueryExecutor:
    """查询执行器：submit() 排队，工作线程流式写 CSV，清理线程回收任务与文件。"""

    def __init__(self, settings: Settings, results_dir: Path | None = None):
        self._settings = settings
        self._db_cfg = settings.mcp.db
        self._results_dir = results_dir or (Path(__file__).parent / "results")
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._store = TaskStore(
            max_tasks=self._db_cfg.async_.max_tasks,
            ttl_seconds=self._db_cfg.async_.ttl_minutes * 60,
        )
        self._jobs: queue.Queue = queue.Queue(maxsize=self._db_cfg.async_.queue_capacity)
        for i in range(self._db_cfg.async_.max_pool_size):
            threading.Thread(target=self._worker, daemon=True, name=f"query-worker-{i}").start()
        self._stopping = threading.Event()
        threading.Thread(target=self._janitor_loop, daemon=True, name="result-janitor").start()

    @property
    def results_dir(self) -> Path:
        return self._results_dir

    def submit(self, sql: str, instance_name: str,
               instance: InstanceSettings, catalog: str | None) -> str:
        """注册任务并排队，返回 queryId。队列满抛 queue.Full，由调用方转成重试提示。"""
        query_id = str(uuid.uuid4())
        self._store.add_running(query_id)
        try:
            self._jobs.put_nowait((query_id, sql, instance_name, instance, catalog))
        except queue.Full:
            self._store.drop(query_id)   # 没排上队，任务快照也得撤掉
            raise
        return query_id

    def get(self, query_id: str) -> dict | None:
        return self._store.get(query_id)

    def close(self) -> None:
        self._stopping.set()

    # ------------------------- 内部：工作与清理 -------------------------
    def _worker(self) -> None:
        while True:
            job = self._jobs.get()
            try:
                self._run_one(*job)
            except BaseException:
                QUERY_LOG.exception("工作线程异常（任务 %s）", job[0])   # 线程绝不能死
            finally:
                self._jobs.task_done()

    def _run_one(self, query_id: str, sql: str, instance_name: str,
                 instance: InstanceSettings, catalog: str | None) -> None:
        result_file = self._results_dir / f"{query_id}.csv"
        started = time.time()
        conn = None
        try:
            conn = database.connect(instance, catalog,
                                    self._db_cfg.pool.connection_timeout)
            cursor = conn.cursor()
            cursor.execute(sql)
            with open(result_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([col[0] for col in cursor.description])   # 表头
                rows = 0
                for row in cursor:          # 游标逐行吐，不攒大列表——内存与行数无关
                    writer.writerow(row)
                    rows += 1
            elapsed_ms = int((time.time() - started) * 1000)
            QUERY_LOG.info("%s %s.%s 行数=%d 耗时=%dms",
                           query_id, instance_name, catalog, rows, elapsed_ms)
            self._store.finish(query_id, status="SUCCESS",
                               file=result_file.name, rows=rows, elapsed_ms=elapsed_ms)
        except BaseException as e:          # 任何漏网异常都不能让任务永远停在 RUNNING
            result_file.unlink(missing_ok=True)   # 半截文件不留尸
            reason = str(e).splitlines()[0]
            QUERY_LOG.warning("%s %s.%s 失败 原因=%s", query_id, instance_name, catalog, reason)
            self._store.finish(query_id, status="FAILED", error=reason)
        finally:
            if conn is not None:
                conn.close()

    def _janitor_loop(self) -> None:
        while not self._stopping.wait(JANITOR_INTERVAL_SECONDS):
            self.cleanup()

    def cleanup(self) -> None:
        """一轮回收：①过期任务 ②过期结果文件 ③目录超容量删最旧。测试直接调这里。"""
        self._store.purge_expired()
        now = time.time()
        ttl = self._db_cfg.result.ttl_minutes * 60
        kept: list[tuple[Path, float, int]] = []
        for path in self._results_dir.glob("*.csv"):
            stat = path.stat()
            if now - stat.st_mtime >= ttl:
                path.unlink(missing_ok=True)
            else:
                kept.append((path, stat.st_mtime, stat.st_size))
        # 容量兜底：正常靠 TTL 回收，这里只兜“TTL 窗口内堆积出海量查询”
        total = sum(size for _, _, size in kept)
        for path, _, size in sorted(kept, key=lambda item: item[1]):   # 最旧的先删
            if total <= self._db_cfg.result.max_total_bytes:
                break
            path.unlink(missing_ok=True)
            total -= size
