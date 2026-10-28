"""冒烟测试：不起真服务器，装配后走内存通道跑全流程。

覆盖：配置加载与环境覆盖层、占位符展开、启动校验、SQL 守卫、
资源（db://databases、db://{db}/tables）、提示词、
异步查询全流程（经 MCP 客户端调用，结构化输出）、任务表 LRU/TTL、
结果文件回收、连接池借出/归还/池满超时。
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

# 本地没有银行凭据：给占位值，让 prod 覆盖层的 ${MYSQL_USER} 等能展开
os.environ.setdefault("MYSQL_USER", "smoke")
os.environ.setdefault("MYSQL_PASSWORD", "smoke")
os.environ.setdefault("DOWNLOAD_BASE_URL", "http://localhost:9001/results")

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(name)s %(message)s")

from pydantic import ValidationError
from sqlalchemy.exc import TimeoutError as SaPoolTimeout
from sqlalchemy.ext.asyncio import create_async_engine

from executor import QueryExecutor, TaskStore
from server import build
from settings import Settings
from sql_guard import validate

# ---------- ① 配置：dev 默认合并 sqlite 演示库，download_base_url 来自 config.dev.yaml ----------
s = Settings()
assert s.server.transport == "stdio"
assert s.server.name == "db-query-server"
assert "demo" in s.databases and s.databases["demo"].dialect == "sqlite"
assert s.databases["demo"].catalogs == ["demo"]
assert s.results.download_base_url.endswith("/results")
print("① 配置加载 OK：transport=stdio，download_base_url =", s.results.download_base_url)

# ---------- ② prod 覆盖层：真实 mysql 实例 + ${MYSQL_USER} 展开 ----------
os.environ["MCP_ENV"] = "prod"
try:
    s2 = Settings()
    bank = s2.databases["bank"]
    assert bank.dialect == "mysql" and bank.username == "smoke"
    assert bank.hosts == ["10.69.43.11:9030", "10.69.43.12:9030", "10.69.43.13:9030"]
    assert "sdls_des" in bank.catalogs and bank.pool_size == 8
    assert s2.server.transport == "http"
    from database import _engine_url
    url = _engine_url(bank)
    host = url.split("@")[1].split(":")[0]
    assert url.startswith("mysql+aiomysql://smoke:smoke@") and host.startswith("10.69.43.")
    assert "charset=utf8mb4" in url
finally:
    os.environ.pop("MCP_ENV")
print("② prod 配置 OK：mysql 主机组与凭据占位符展开，URL =", url)

# ---------- ③ 启动期校验 ----------
from settings import ResultsSettings, ServerSettings, QuerySettings, DatabaseSettings

def _settings(**kw) -> Settings:
    return Settings(
        server=kw.get("server") or ServerSettings(),
        query=kw.get("query") or QuerySettings(),
        results=kw.get("results") or ResultsSettings(download_base_url="http://x"),
        databases=kw.get("databases")
        or {"demo": {"dialect": "sqlite", "path": "demo.db", "catalogs": ["demo"]}},
    )

for name, bad in [
    ("results.ttl <= query.task_ttl 被拦",
     dict(query={"task_ttl_minutes": 50},
          results={"ttl_minutes": 40, "download_base_url": "http://x"})),
    ("http 模式缺 download_base_url 被拦",
     dict(server={"transport": "http"}, results={"download_base_url": ""})),
    ("缺 dialect 被拦", dict(databases={"x": {"catalogs": ["a"]}})),
    ("sqlite 缺 path 被拦", dict(databases={"x": {"dialect": "sqlite", "catalogs": ["a"]}})),
]:
    try:
        _settings(**bad)
        raise SystemExit(f"!! 未拦截：{name}")
    except ValidationError as e:
        print(f"③ 校验 OK：{name}")

# ---------- ④ SQL 守卫 ----------
for bad in ["DELETE FROM orders", "UPDATE orders SET amount = 0",
            "SELECT * FROM orders FOR UPDATE", "SELECT SLEEP(5) FROM orders",
            "SELECT 1 FROM orders; SELECT 2"]:
    try:
        validate(bad, "sqlite")
        raise SystemExit("!! 未拦截: " + bad)
    except ValueError as e:
        print("④ 拦截 OK:", bad[:28].ljust(28), "->", str(e)[:32])

# ---------- ⑤ 全流程：资源 + 提示词 + MCP 客户端调用 ----------
async def flow() -> None:
    from fastmcp import Client

    mcp = build(s)
    async with Client(mcp) as client:
        tools = sorted(t.name for t in await client.list_tools())
        assert tools == ["get_query_db_async_result", "query_db_async"], tools

        # 资源：可查的库 + 表清单（URI 模板）
        dbs = await client.read_resource("db://databases")
        assert "demo" in str(dbs), dbs
        tables = await client.read_resource("db://demo/tables")
        assert "orders" in str(tables), tables
        print("⑤ 资源 OK：db://databases ->", str(dbs)[:40], "| db://demo/tables ->", str(tables)[:30])

        # 提示词模板
        prompts = {p.name for p in await client.list_prompts()}
        assert "query-assistant" in prompts, prompts
        rendered = await client.get_prompt("query-assistant",
                                           {"question": "各状态订单数", "database": "demo"})
        assert "各状态订单数" in str(rendered)
        print("⑤ 提示词 OK：query-assistant 可用")

        r = await client.call_tool("query_db_async", {
            "sql": "SELECT status, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total "
                   "FROM orders GROUP BY status",
            "database": "demo"})
        assert r.data.success, r.data            # 结构化输出：直接拿 ToolReply 字段
        query_id = r.data.data

        for _ in range(100):
            r2 = await client.call_tool("get_query_db_async_result", {"query_id": query_id})
            if r2.data.message and "仍在执行" in r2.data.message:
                await asyncio.sleep(0.05)
                continue
            break
        assert r2.data.success, r2.data.message
        print("⑤ 工具返回:", r2.data.message, "->", r2.data.data)

        filename = r2.data.data.split("/")[-1]
        content = (Path(s.results.dir) / filename).read_text(encoding="utf-8")
        print("--- 结果文件 ---")
        print(content)
        assert "cnt" in content and "PAID" in content

        # 失败路径：未知库 / 危险 SQL
        r3 = await client.call_tool("query_db_async",
                                    {"sql": "SELECT 1", "database": "nope"})
        assert not r3.data.success and "未知数据库" in r3.data.message
        r4 = await client.call_tool("query_db_async",
                                    {"sql": "DELETE FROM orders", "database": "demo"})
        assert not r4.data.success
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
    small.query.workers = 1
    small.query.queue_size = 2
    small.results.ttl_minutes = 1                 # 60s TTL
    small.results.max_total_bytes = 150           # 容量兜底：三个文件 50+50+70 才会超
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

# ---------- ⑧ 连接池（SQLAlchemy 异步引擎）：借出/归还/池满超时 ----------
async def pool_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{Path(tmp).as_posix()}/pool.db",
            pool_size=1, max_overflow=0, pool_timeout=0.3, pool_pre_ping=True,
        )
        held = await engine.connect()        # 占满池上限（1）
        try:
            await engine.connect()
            raise SystemExit("!! 池上限没拦住")
        except SaPoolTimeout:
            pass
        await held.close()                   # 归还
        async with engine.connect() as conn:   # 归还后立刻可再借
            assert (await conn.exec_driver_sql("SELECT 1")).fetchone() == (1,)
        await engine.dispose()

asyncio.run(pool_test())
print("⑧ 连接池 OK：借出 / 归还 / 池满超时")

print("SMOKE PASS")
