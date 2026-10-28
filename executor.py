"""数据处理层：异步任务表 + asyncio 任务池 + 结果文件落盘与回收。

全链路单事件循环（FastMCP 的 stdio/http 传输都在 asyncio 上），没有任何
线程和锁——任务表只在事件循环里被访问，天然串行：
  max-pool-size   工作协程数 = 同时在跑的查询上限
  queue-capacity  asyncio.Queue 上限；满了 submit 抛 QueueFull
                  （调用方转成“稍后重试”），绝不让请求无限堆积
  ttl-minutes     任务在内存里的保留时长，查一次状态刷新一次“最近访问”
  max-tasks       任务表上限，超出按最久未访问淘汰（RUNNING 的不淘汰）
  result.*        结果文件 TTL 与目录总容量兜底，由后台协程周期回收

行数刻意不回传给模型（只进 QUERY_LOG 服务端日志）——该不该取明细应该在
写 SQL 时决定，模型一旦知道行数就会纠结要不要拉全量（Java 版同理）。
"""
from __future__ import annotations

import asyncio
import csv
import logging
import time
import uuid
from collections import OrderedDict
from pathlib import Path

import database
from settings import InstanceSettings, Settings

QUERY_LOG = logging.getLogger("QUERY_LOG")
JANITOR_INTERVAL_SECONDS = 60


class TaskStore:
    """queryId → 任务快照。只在事件循环里被访问（单线程），不需要锁。

    快照字段：status(RUNNING/SUCCESS/FAILED) / time / file / rows / elapsed_ms / error
    """

    def __init__(self, max_tasks: int, ttl_seconds: float):
        self._tasks: OrderedDict[str, dict] = OrderedDict()
        self._max_tasks = max_tasks
        self._ttl_seconds = ttl_seconds

    def add_running(self, query_id: str) -> None:
        self._evict()
        self._tasks[query_id] = {"status": "RUNNING", "time": time.time()}

    def finish(self, query_id: str, **fields: object) -> None:
        self._tasks[query_id] = {"status": fields.pop("status"), "time": time.time(), **fields}
        self._tasks.move_to_end(query_id)

    def drop(self, query_id: str) -> None:
        self._tasks.pop(query_id, None)

    def get(self, query_id: str) -> dict | None:
        """取任务快照（新 dict，不暴露内部引用）。过期即删并返回 None。"""
        task = self._tasks.get(query_id)
        if task is None:
            return None
        if task["status"] != "RUNNING" and time.time() - task["time"] > self._ttl_seconds:
            del self._tasks[query_id]
            return None
        self._tasks.move_to_end(query_id)   # 最近访问的活得久
        return dict(task)

    def purge_expired(self) -> int:
        """清掉所有过期任务，返回清理数量（后台协程周期调用）。"""
        now = time.time()
        expired = [k for k, t in self._tasks.items()
                   if t["status"] != "RUNNING" and now - t["time"] > self._ttl_seconds]
        for k in expired:
            del self._tasks[k]
        return len(expired)

    def _evict(self) -> None:
        """超上限时按最久未访问淘汰（OrderedDict 头部即最旧端，跳过 RUNNING）。"""
        while len(self._tasks) >= self._max_tasks:
            victim = next((k for k, t in self._tasks.items() if t["status"] != "RUNNING"), None)
            if victim is None:
                break   # 全在执行中：宁可不收新任务也不误删 RUNNING
            del self._tasks[victim]


class QueryExecutor:
    """查询执行器：submit() 排队，工作协程流式写 CSV，后台协程回收任务与文件。

    工作协程在第一次 submit 时才启动——那时才一定有正在跑的事件循环。
    """

    def __init__(self, settings: Settings, results_dir: Path | None = None):
        self._settings = settings
        self._db_cfg = settings.mcp.db
        self._results_dir = results_dir or (Path(__file__).parent / "results")
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._store = TaskStore(
            max_tasks=self._db_cfg.async_.max_tasks,
            ttl_seconds=self._db_cfg.async_.ttl_minutes * 60,
        )
        self._jobs: asyncio.Queue = asyncio.Queue(maxsize=self._db_cfg.async_.queue_capacity)
        self._workers: list[asyncio.Task] = []
        self._stopped = False

    @property
    def results_dir(self) -> Path:
        return self._results_dir

    async def submit(self, sql: str, instance_name: str,
                     instance: InstanceSettings, catalog: str | None) -> str:
        """注册任务并排队，返回 queryId。队列满抛 asyncio.QueueFull。"""
        self._ensure_workers()
        query_id = str(uuid.uuid4())
        self._store.add_running(query_id)
        try:
            self._jobs.put_nowait((query_id, sql, instance_name, instance, catalog))
        except asyncio.QueueFull:
            self._store.drop(query_id)   # 没排上队，任务快照也得撤掉
            raise
        return query_id

    def get(self, query_id: str) -> dict | None:
        return self._store.get(query_id)

    def close(self) -> None:
        self._stopped = True
        for worker in self._workers:
            worker.cancel()

    # ------------------------- 内部：工作与回收 -------------------------
    def _ensure_workers(self) -> None:
        if self._workers:
            return
        self._workers = [
            asyncio.create_task(self._worker(), name=f"query-worker-{i}")
            for i in range(self._db_cfg.async_.max_pool_size)
        ]
        asyncio.create_task(self._janitor_loop(), name="result-janitor")

    async def _worker(self) -> None:
        while True:
            job = await self._jobs.get()
            try:
                await self._run_one(*job)
            except asyncio.CancelledError:
                raise
            except BaseException:
                QUERY_LOG.exception("工作协程异常（queryId %s）", job[0])   # 协程绝不能死
            finally:
                self._jobs.task_done()

    async def _run_one(self, query_id: str, sql: str, instance_name: str,
                       instance: InstanceSettings, catalog: str | None) -> None:
        result_file = self._results_dir / f"{query_id}.csv"
        started = time.time()
        try:
            # with 退出即归还连接：正常跑完归还复用；中途抛异常则作废，绝不带病回池
            async with database.borrow(self._settings, instance_name, instance, catalog) as conn:
                with open(result_file, "w", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)

                    def write_all(sync_conn) -> int:
                        # greenlet 里的同步连接：原始 SQL 直达驱动（不走 text() 的
                        # :参数 解析），stream_results 服务端游标逐行拉取
                        result = sync_conn.execution_options(
                            stream_results=True).exec_driver_sql(sql)
                        writer.writerow(list(result.keys()))          # 表头
                        rows = 0
                        for row in result:      # 不攒大列表——内存与行数无关
                            writer.writerow(row)
                            rows += 1
                        return rows

                    rows = await conn.run_sync(write_all)
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

    async def _janitor_loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(JANITOR_INTERVAL_SECONDS)
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
