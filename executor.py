"""数据处理层：任务表 + asyncio 任务池 + 结果文件落盘与回收。

全链路单事件循环（stdio / http 两种传输都跑在 asyncio 上），没有任何
线程和锁——任务表只在事件循环里被访问，天然串行：
  query.workers        工作协程数 = 同时在跑的查询上限
  query.queue-size     asyncio.Queue 上限；满了 submit 抛 QueueFull
                       （调用方转成“稍后重试”），绝不让请求无限堆积
  query.task-ttl-minutes 任务在内存里的保留时长，查一次状态刷新一次“最近访问”
  query.max-tasks      任务表上限，超出按最久未访问淘汰（RUNNING 的不淘汰）
  results.*            结果文件 TTL 与目录总容量兜底，由后台协程周期回收

结果返回策略：行数 ≤ query.inline_max_rows 的小结果直接内联在工具回复里
（边写盘边缓冲，超过阈值立即弃缓冲——内存开销有上界）；更大的结果只给
文件/下载链接，行数只进服务端日志——模型不知道行数就不会纠结拉全量。
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
from settings import BASE_DIR, DatabaseSettings, Settings

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
        self._query_cfg = settings.query
        self._results_cfg = settings.results
        self._results_dir = results_dir or (BASE_DIR / settings.results.dir)
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._store = TaskStore(
            max_tasks=self._query_cfg.max_tasks,
            ttl_seconds=self._query_cfg.task_ttl_minutes * 60,
        )
        self._jobs: asyncio.Queue = asyncio.Queue(maxsize=self._query_cfg.queue_size)
        self._workers: list[asyncio.Task] = []
        self._stopped = False

    @property
    def results_dir(self) -> Path:
        return self._results_dir

    async def submit(self, sql: str, db_name: str,
                     db: DatabaseSettings, catalog: str | None) -> str:
        """注册任务并排队，返回 queryId。队列满抛 asyncio.QueueFull。"""
        self._ensure_workers()
        query_id = str(uuid.uuid4())
        self._store.add_running(query_id)
        try:
            self._jobs.put_nowait((query_id, sql, db_name, db, catalog))
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
            for i in range(self._query_cfg.workers)
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

    async def _run_one(self, query_id: str, sql: str, db_name: str,
                       db: DatabaseSettings, catalog: str | None) -> None:
        result_file = self._results_dir / f"{query_id}.csv"
        started = time.time()
        try:
            # with 退出即归还连接：正常跑完归还复用；中途抛异常则作废，绝不带病回池
            async with database.borrow(self._settings, db_name, db, catalog) as conn:
                # 目录可能在运行中被清理（启动时建过不算数），写前兜底重建
                self._results_dir.mkdir(parents=True, exist_ok=True)
                with open(result_file, "w", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    threshold = self._query_cfg.inline_max_rows

                    def write_all(sync_conn) -> tuple[list[str], int, list | None]:
                        # greenlet 里的同步连接：原始 SQL 直达驱动（不走 text() 的
                        # :参数 解析），stream_results 服务端游标逐行拉取
                        result = sync_conn.execution_options(
                            stream_results=True).exec_driver_sql(sql)
                        columns = list(result.keys())
                        writer.writerow(columns)                        # 表头
                        rows = 0
                        # 小结果缓冲：先攒着，一旦超过阈值立即弃掉——
                        # 内存开销有上界，大查询依旧与行数无关
                        inline: list | None = [] if threshold > 0 else None
                        for row in result:
                            writer.writerow(row)
                            rows += 1
                            if inline is not None:
                                inline.append(list(row))
                                if len(inline) > threshold:
                                    inline = None
                        return columns, rows, inline

                    columns, rows, inline = await conn.run_sync(write_all)
            elapsed_ms = int((time.time() - started) * 1000)
            QUERY_LOG.info("%s %s.%s 行数=%d 耗时=%dms%s",
                           query_id, db_name, catalog, rows, elapsed_ms,
                           "" if inline is None else " 内联展示")
            if inline is not None:
                result_file.unlink(missing_ok=True)   # 小结果不留文件
                self._store.finish(query_id, status="SUCCESS", rows=rows,
                                   elapsed_ms=elapsed_ms,
                                   table={"columns": columns, "rows": inline})
            else:
                self._store.finish(query_id, status="SUCCESS", rows=rows,
                                   elapsed_ms=elapsed_ms, file=result_file.name)
        except BaseException as e:          # 任何漏网异常都不能让任务永远停在 RUNNING
            result_file.unlink(missing_ok=True)   # 半截文件不留尸
            reason = str(e).splitlines()[0]
            QUERY_LOG.warning("%s %s.%s 失败 原因=%s", query_id, db_name, catalog, reason)
            self._store.finish(query_id, status="FAILED", error=reason)

    async def _janitor_loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(JANITOR_INTERVAL_SECONDS)
            self.cleanup()

    def cleanup(self) -> None:
        """一轮回收：①过期任务 ②过期结果文件 ③目录超容量删最旧。测试直接调这里。"""
        self._store.purge_expired()
        now = time.time()
        ttl = self._results_cfg.ttl_minutes * 60
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
            if total <= self._results_cfg.max_total_bytes:
                break
            path.unlink(missing_ok=True)
            total -= size
