import csv
import sqlite3
import sys
import threading
import time
import uuid
from pathlib import Path

from fastmcp import FastMCP

from sql_guard import validate

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "demo.db"
RESULTS_DIR = BASE_DIR / "results"
MAX_CONCURRENT = 5                          # 并发上限，保护数据库（对应 Java 版队列拒绝）

mcp = FastMCP("db-query-server")

_tasks: dict = {}                           # queryId → 任务快照，两个工具间唯一的交接物
_lock = threading.Lock()                    # 后台线程和工具线程共用这个 dict，必须加锁
_slots = threading.Semaphore(MAX_CONCURRENT)


def _run_query(query_id: str, safe_sql: str) -> None:
    """后台线程：执行查询 → 逐行写 csv → 回写状态。"""
    result_file = RESULTS_DIR / f"{query_id}.csv"
    try:
        conn = sqlite3.connect(DB_PATH)
        try:
            cursor = conn.execute(safe_sql)
            with open(result_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([d[0] for d in cursor.description])   # 表头
                count = 0
                for row in cursor:          # 游标逐行吐，不攒大列表——内存与行数无关
                    writer.writerow(row)
                    count += 1
        finally:
            conn.close()
        print(f"[query] {query_id} 完成，{count} 行", file=sys.stderr)  # 日志走 stderr，别碰 stdout！
        with _lock:
            _tasks[query_id] = {"status": "SUCCESS", "time": time.time(), "file": result_file.name}
    except BaseException as e:              # 任何漏网异常都不能让任务永远停在 RUNNING
        result_file.unlink(missing_ok=True) # 半截文件不留尸
        with _lock:
            _tasks[query_id] = {"status": "FAILED", "time": time.time(),
                                "error": str(e).splitlines()[0]}
    finally:
        _slots.release()


def _ok(data, message=None):
    return {"success": True, "message": message, "data": data}


def _fail(message):
    return {"success": False, "message": message, "data": None}


@mcp.tool
def query_db_async(sql: str) -> dict:
    """异步执行只读 SQL，立即返回查询ID（JSON：success/message/data）。
    用法：① 拿到 queryId；② 调 get_query_db_async_result 查状态。
    重要：能用 COUNT / SUM / GROUP BY 在 SQL 里算完的，就不要取全量明细。"""
    try:
        safe_sql = validate(sql)
    except ValueError as e:
        return _fail(str(e) + "\n请修正 SQL 后重试。")
    if not _slots.acquire(blocking=False):
        return _fail(f"当前并发查询已满（{MAX_CONCURRENT}），请稍后重试。")
    query_id = str(uuid.uuid4())
    with _lock:
        _tasks[query_id] = {"status": "RUNNING", "time": time.time()}
    threading.Thread(target=_run_query, args=(query_id, safe_sql),
                    daemon=True, name=f"query-{query_id[:8]}").start()
    return _ok(query_id, f"查询已提交，queryId={query_id}。"
                        "稍后调用 get_query_db_async_result 传入该 queryId 获取结果。")

@mcp.tool
def get_query_db_async_result(query_id: str) -> dict:
    """按 queryId 查询任务状态。RUNNING→等几秒再查（不要重复提交）；
    FAILED→附原因；SUCCESS→返回结果文件信息。"""
    task = _tasks.get((query_id or "").strip())
    if task is None:
        return _fail(f"查询 {query_id} 不存在或已过期，请重新提交。")
    if task["status"] == "RUNNING":
        waited = int((time.time() - task["time"]) * 1000)
        return _fail(f"查询仍在执行（已提交 {waited} ms），请稍后重试，不要重复提交。")
    if task["status"] == "FAILED":
        return _fail(f"查询失败：{task['error']}\n请修正 SQL 后重新提交。")
    # 注意：行数刻意不回传！模型一旦知道行数就会纠结“要不要取明细”，
    # 而该不该取明细应该在写 SQL 时决定（Java 版 AsyncTask.rowCount 只进服务端日志）
    return _ok(task["file"], "查询完成，结果文件已生成。")


if __name__ == "__main__":
    RESULTS_DIR.mkdir(exist_ok=True)
    mcp.run()