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
import shutil
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
from server import QUERY_LOG, build
from settings import Settings
from sql_guard import validate

# ---------- ① 配置：dev 默认合并 sqlite 演示库，download_base_url 来自 config.dev.yaml ----------
s = Settings()
assert s.server.transport in ("stdio", "http")   # 传输方式随 dev 配置走，不锁死
assert s.server.name == "db-query-server"
assert "demo" in s.databases and s.databases["demo"].dialect == "sqlite"
assert s.databases["demo"].catalogs == ["demo"]
assert s.results.download_base_url.endswith("/results")
print("① 配置加载 OK：transport=%s，download_base_url = %s"
      % (s.server.transport, s.results.download_base_url))

# ---------- ② prod 覆盖层：真实 mysql 实例 + ${MYSQL_USER} 展开 ----------
# 只断言不变量（占位符展开、URL 结构、主机来自配置），不锁死具体 IP/库名，
# 改 config.prod.yaml 不需要改这里
os.environ["MCP_ENV"] = "prod"
try:
    s2 = Settings()
    bank = s2.databases["bank"]
    assert bank.dialect == "mysql" and bank.username == "smoke"   # ${MYSQL_USER} 展开
    assert bank.hosts and all(h.count(":") == 1 for h in bank.hosts)
    assert bank.catalogs
    assert bank.pool_size == 8
    assert s2.server.transport == "http"
    from database import _engine_url
    url = _engine_url(bank)
    host = url.split("@")[1].split(":")[0]
    assert url.startswith("mysql+aiomysql://smoke:smoke@")
    assert host in [h.split(":")[0] for h in bank.hosts]          # 主机取自配置
    assert "charset=utf8mb4" in url
finally:
    os.environ.pop("MCP_ENV")
print("② prod 配置 OK：凭据占位符展开，URL =", url)

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

        # 提示词模板：不带库 → 返回库清单让用户挑；带库 → 附表清单；错库 → 引导重选
        prompts = {p.name for p in await client.list_prompts()}
        assert "query-assistant" in prompts, prompts
        r_no_db = str(await client.get_prompt("query-assistant",
                                              {"question": "各状态订单数"}))
        assert "当前可用的库" in r_no_db and "demo" in r_no_db
        r_demo = str(await client.get_prompt("query-assistant",
                                             {"question": "各状态订单数", "database": "demo"}))
        assert "orders" in r_demo and "各状态订单数" in r_demo
        r_bad = str(await client.get_prompt("query-assistant",
                                            {"question": "x", "database": "nope"}))
        assert "未知数据库" in r_bad and "demo" in r_bad
        print("⑤ 提示词 OK：未选库→列库清单；选库→附表清单；错库→引导重选")

        async def poll(query_id: str):
            """轮询到出结果为止，返回最终一次的 ToolReply。"""
            for _ in range(100):
                r = await client.call_tool("get_query_db_async_result",
                                           {"query_id": query_id})
                if r.data.message and "仍在执行" in r.data.message:
                    await asyncio.sleep(0.05)
                    continue
                return r
            raise SystemExit("!! 轮询超时")

        # 小结果（GROUP BY 2 行 ≤ 5）→ 内联展示，不落文件
        r = await client.call_tool("query_db_async", {
            "sql": "SELECT status, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total "
                   "FROM orders GROUP BY status",
            "database": "demo"})
        assert r.data.success, r.data            # 结构化输出：直接拿 ToolReply 字段
        r2 = await poll(r.data.data)
        assert r2.data.success, r2.data.message
        assert r2.data.table is not None
        assert r2.data.table.columns == ["status", "cnt", "total"]
        assert 1 <= len(r2.data.table.rows) <= 5          # 行数随数据变，只断言在阈值内
        assert any("PAID" in str(row) for row in r2.data.table.rows)
        print("⑤ 小结果内联 OK:", r2.data.table.columns, r2.data.table.rows)

        # 大结果（8 行 > 5）→ 落 CSV 文件
        rb = await client.call_tool("query_db_async", {
            "sql": "SELECT id, customer, amount FROM orders LIMIT 8",
            "database": "demo"})
        assert rb.data.success, rb.data
        r2b = await poll(rb.data.data)
        assert r2b.data.success, r2b.data.message
        assert r2b.data.table is None and r2b.data.data.endswith(".csv")
        filename = r2b.data.data.split("/")[-1]
        content = (Path(s.results.dir) / filename).read_text(encoding="utf-8")
        print("--- 大结果 CSV 文件 ---")
        print(content)
        assert "customer" in content and len(content.splitlines()) == 9   # 表头 + 8 行

        # results 目录被删也能自愈：写文件前会兜底重建目录
        shutil.rmtree(s.results.dir, ignore_errors=True)
        rc = await client.call_tool("query_db_async", {
            "sql": "SELECT id, customer, amount FROM orders LIMIT 8",
            "database": "demo"})
        r2c = await poll(rc.data.data)
        assert r2c.data.success and r2c.data.data.endswith(".csv")
        print("⑤ 目录自愈 OK：results 被删后查询照常落盘")

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

# ---------- ⑨ http 下载端点：命中/未命中/非法 + 调用日志 ----------
import httpx

class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())

async def download_route_test() -> None:
    s_http = s.model_copy(deep=True)
    s_http.server.transport = "http"
    capture = _LogCapture()
    QUERY_LOG.addHandler(capture)
    try:
        app = build(s_http).http_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/results/evil.csv")
            assert r.status_code == 400 and "非法文件名" in r.text
            r = await client.get("/results/00000000-0000-0000-0000-000000000009.csv")
            assert r.status_code == 404 and "不存在或已过期" in r.text
            real = next(p.name for p in sorted(Path(s.results.dir).glob("*.csv"),
                                               key=lambda p: p.stat().st_mtime) if p.stat().st_size > 0)
            r = await client.get(f"/results/{real}")
            assert r.status_code == 200 and len(r.text.splitlines()) >= 2   # 有表头有数据
        joined = "\n".join(capture.messages)
        assert "下载拒绝" in joined and "下载未命中" in joined and "下载 来源=" in joined
    finally:
        QUERY_LOG.removeHandler(capture)

asyncio.run(download_route_test())
print("⑨ 下载端点 OK：命中 200 / 未命中 404 / 非法 400，且每次下载都有调用日志")

print("SMOKE PASS")
