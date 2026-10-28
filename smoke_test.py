"""冒烟测试：不起真服务器，装配后走内存通道跑全流程。

覆盖：配置加载与 profile 覆盖、占位符展开、启动期交叉校验、SQL 守卫、
异步查询全流程（经 MCP 客户端调用）、任务表 LRU/TTL、结果文件回收。
运行：python smoke_test.py
"""
import asyncio
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, ".")

# 本地没有银行凭据：给占位值，让 prod 段的 ${MYSQL_USER} 等能展开（dev 用不到它们）
os.environ.setdefault("MYSQL_USER", "smoke")
os.environ.setdefault("MYSQL_PASSWORD", "smoke")
os.environ.setdefault("DOWNLOAD_BASE_URL", "http://localhost:9001/results")

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(name)s %(message)s")

from pydantic import ValidationError

from executor import QueryExecutor, TaskStore
from server import build
from settings import DbSettings, Settings
from sql_guard import validate

# ---------- ① 配置：dev 默认合并 sqlite-demo，download-base-url 来自 application-dev.yml ----------
s = Settings()
assert s.spring.profiles.active == "dev"
assert s.spring.ai.mcp.server.protocol == "stdio"
assert "sqlite-demo" in s.mcp.db.instances
assert (s.mcp.db.result.download_base_url or "").endswith("/results")
demo = s.mcp.db.instances["sqlite-demo"]
assert demo.dialect == "sqlite" and "demo" in demo.catalogs
print("① 配置加载 OK：profile=dev，download-base-url =", s.mcp.db.result.download_base_url)

# ---------- ② prod profile：真实 mysql 实例 + ${MYSQL_USER} 展开 ----------
os.environ["SPRING_PROFILES_ACTIVE"] = "prod"
try:
    s2 = Settings()
    mysql = s2.mcp.db.instances["mysql"]
    assert mysql.dialect == "mysql" and mysql.username == "smoke"
    assert "sdls_des" in mysql.catalogs and "sdp_isl_des" in mysql.catalogs
    assert mysql.maximum_pool_size == 8            # 兼容字段原样读进来
    assert s2.spring.ai.mcp.server.protocol == "streamable"   # prod 未覆盖 → 用主配置
finally:
    os.environ.pop("SPRING_PROFILES_ACTIVE")
print("② prod 配置 OK：mysql 实例凭据占位符展开为", mysql.username)

# ---------- ③ 启动期交叉校验 ----------
def _db_cfg(async_ttl: int, result_ttl: int, base: str = "http://x") -> DbSettings:
    return DbSettings(**{
        "async": {"ttl-minutes": async_ttl},
        "result": {"ttl-minutes": result_ttl, "download-base-url": base},
        "instances": {"sqlite": {"url": "jdbc:sqlite:t.db"}},   # 键名=方言名，dialect 可省
    })

for name, kwargs, expect in [
    ("result.ttl <= async.ttl 被拦", dict(async_ttl=50, result_ttl=40), "必须大于"),
    ("download-base-url 缺失被拦", dict(async_ttl=30, result_ttl=40, base=""), "download-base-url 缺失"),
    ("自定义实例名缺 dialect 被拦", None, "必须显式配置 dialect"),
]:
    try:
        if kwargs is None:
            DbSettings(**{"result": {"download-base-url": "http://x"},
                          "instances": {"pg-report": {"url": "jdbc:postgresql://h/db"}}})
        else:
            _db_cfg(**kwargs)
        raise SystemExit(f"!! 未拦截：{name}")
    except ValidationError as e:
        assert expect in str(e), str(e)
        print(f"③ 交叉校验 OK：{name}")
assert _db_cfg(async_ttl=30, result_ttl=40).instances["sqlite"].dialect == "sqlite"
print("③ 键名即方言时 dialect 自动补全 OK")

# ---------- ④ SQL 守卫（sqlite 方言） ----------
for bad in ["DELETE FROM orders", "UPDATE orders SET amount = 0",
            "SELECT * FROM orders FOR UPDATE", "SELECT SLEEP(5) FROM orders",
            "SELECT 1 FROM orders; SELECT 2"]:
    try:
        validate(bad, "sqlite")
        raise SystemExit("!! 未拦截: " + bad)
    except ValueError as e:
        print("④ 拦截 OK:", bad[:28].ljust(28), "->", str(e)[:32])

# ---------- ⑤ 全流程：MCP 客户端内存调用 ----------
async def flow() -> None:
    from fastmcp import Client

    mcp = build(s)
    async with Client(mcp) as client:
        tools = sorted(t.name for t in await client.list_tools())
        assert tools == ["get_query_db_async_result", "query_db_async"], tools

        r = await client.call_tool("query_db_async", {
            "sql": "SELECT status, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total "
                   "FROM orders GROUP BY status",
            "database": "demo"})
        assert r.data["success"], r.data
        query_id = r.data["data"]

        for _ in range(100):
            r2 = await client.call_tool("get_query_db_async_result", {"query_id": query_id})
            if "仍在执行" in r2.data["message"]:
                await asyncio.sleep(0.05)
                continue
            break
        assert r2.data["success"], r2.data["message"]
        print("⑤ 工具返回:", r2.data["message"], "->", r2.data["data"])

        filename = r2.data["data"].split("/")[-1]
        content = (Path("results") / filename).read_text(encoding="utf-8")
        print("--- 结果文件 ---")
        print(content)
        assert "cnt" in content and "PAID" in content

        # 失败路径：未知库 / 危险 SQL
        r3 = await client.call_tool("query_db_async",
                                    {"sql": "SELECT 1", "database": "nope"})
        assert not r3.data["success"] and "未知数据库" in r3.data["message"]
        r4 = await client.call_tool("query_db_async",
                                    {"sql": "DELETE FROM orders", "database": "demo"})
        assert not r4.data["success"]
        print("⑤ 失败路径 OK：未知库与危险 SQL 都被礼貌拒绝")

asyncio.run(flow())

# ---------- ⑥ 任务表：LRU 淘汰 + TTL 过期 ----------
store = TaskStore(max_tasks=3, ttl_seconds=0.1)
store.add_running("a"); store.finish("a", status="SUCCESS", file="a.csv")
store.add_running("b"); store.finish("b", status="SUCCESS", file="b.csv")
store.add_running("c")                                  # RUNNING，不参与淘汰
store.add_running("d")                                  # 超上限 → 淘汰最久未访问的 a
assert store.get("a") is None and store.get("b") is not None
assert store.get("c") is not None and store.get("d") is not None
time.sleep(0.15)
assert store.get("b") is None                           # TTL 到期即“不存在或已过期”
print("⑥ 任务表 OK：LRU 淘汰 + TTL 过期")

# ---------- ⑦ 结果文件回收：TTL 删除 + 容量兜底 ----------
with tempfile.TemporaryDirectory() as tmp:
    small = s.model_copy(deep=True)
    small.mcp.db.async_.max_pool_size = 1
    small.mcp.db.async_.queue_capacity = 2
    small.mcp.db.result.ttl_minutes = 1                 # 60s TTL
    small.mcp.db.result.max_total_bytes = 150           # 容量兜底：三个文件 50+50+70 才会超
    ex = QueryExecutor(small, results_dir=Path(tmp))
    try:
        def fake(name: str, text: str, age: float) -> Path:
            p = Path(tmp) / f"00000000-0000-0000-0000-{name}.csv"
            p.write_text(text, encoding="utf-8")
            t = time.time() - age
            os.utime(p, (t, t))
            return p

        old = fake("000000000001", "x" * 50, age=9999)  # TTL 之外
        mid = fake("000000000002", "y" * 50, age=9)     # TTL 内、最旧
        new = fake("000000000003", "z" * 60, age=0)
        ex.cleanup()
        assert not old.exists()                         # ① TTL 过期被删
        assert mid.exists() and new.exists()            # 110 ≤ 150，容量不触发
        big = fake("000000000004", "w" * 70, age=0)     # 110+70 > 150 → 删最旧的 mid
        ex.cleanup()
        assert not mid.exists()
        assert new.exists() and big.exists()            # ② 容量兜底只删到够为止
        print("⑦ 结果回收 OK：TTL 删除 + 容量兜底")
    finally:
        ex.close()

# ---------- ⑧ 连接池（SQLAlchemy QueuePool）：借出/归还/池满超时 ----------
from sqlalchemy import create_engine
from sqlalchemy.exc import TimeoutError as SaPoolTimeout

with tempfile.TemporaryDirectory() as tmp:
    engine = create_engine(
        f"sqlite:///{Path(tmp).as_posix()}/pool.db",
        pool_size=1, max_overflow=0, pool_timeout=0.3, pool_pre_ping=True,
    )
    held = engine.connect()                  # 占满池上限（1）
    try:
        engine.connect()
        raise SystemExit("!! 池上限没拦住")
    except SaPoolTimeout:
        pass
    held.close()                             # 归还
    with engine.connect() as conn:           # 归还后立刻可再借
        assert conn.exec_driver_sql("SELECT 1").fetchone() == (1,)
    engine.dispose()
    print("⑧ 连接池 OK：借出 / 归还 / 池满超时（SQLAlchemy QueuePool）")

print("SMOKE PASS")
