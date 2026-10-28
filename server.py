"""入口：装配 FastMCP 服务器并运行。

FastMCP 特性的落点：
  instructions      服务器级使用说明（客户端启动时就能看到）
  Context           工具内主动给客户端发实时消息（ctx.info）
  @mcp.resource     资源 + URI 模板：db://databases 浏览可查的库，
                    db://{database}/tables 浏览库里的表——模型写 SQL 前先看这里
  @mcp.prompt       query-assistant 提示词模板
  LoggingMiddleware 每次工具/资源调用自动一行日志（QUERY_LOG）
  pydantic 返回类型  工具输出自动生成结构化 schema（ToolReply）

各文件分工（一个文件一件事）：
  settings.py   配置读取（pydantic-settings 读 config/config.yaml）
  database.py   数据访问（库路由 + 异步连接池 + 表清单）
  executor.py   数据处理（任务表 / asyncio 任务池 / CSV 流式落盘 / 回收）
  sql_guard.py  SQL 守卫（只放行单条只读 SELECT）
  server.py     本文件：装配 + 入口

运行：python server.py            （默认 dev：stdio）
      MCP_ENV=prod python server.py（prod：http，监听 server.port）
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
import time

from fastmcp import Context, FastMCP
from fastmcp.server.middleware.logging import LoggingMiddleware
from pydantic import BaseModel
from starlette.responses import FileResponse, PlainTextResponse

from database import list_tables, route as route_database
from executor import QueryExecutor
from settings import Settings
from sql_guard import validate

QUERY_LOG = logging.getLogger("QUERY_LOG")
# 结果文件名固定是 uuid.csv，白名单外的名字一律拒绝——防路径穿越
SAFE_FILENAME = re.compile(r"^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\.csv$")


class ToolReply(BaseModel):
    """所有工具的统一返回：success / message / data。"""

    success: bool
    message: str = ""
    data: str | None = None


def _fail(message: str) -> ToolReply:
    return ToolReply(success=False, message=message)


def build(settings: Settings | None = None, executor: QueryExecutor | None = None) -> FastMCP:
    """装配出完整的 FastMCP 实例（冒烟测试也走这里）。"""
    settings = settings or Settings()
    executor = executor or QueryExecutor(settings)
    is_http = settings.server.transport == "http"
    download_base = settings.results.download_base_url.rstrip("/")

    mcp = FastMCP(
        settings.server.name,
        instructions=(
            "只读数据库查询服务。用法：① 读资源 db://databases 查看可用的库；"
            "② 需要表名时读 db://{库名}/tables；③ 调 query_db_async 提交 SQL，"
            "拿到 queryId；④ 调 get_query_db_async_result 轮询，成功后给结果文件。"
            "重要：能用 COUNT / SUM / GROUP BY 在 SQL 里算完的，就不要取全量明细；"
            "结果落成 CSV 文件，不会整包返回。"
        ),
    )
    mcp.add_middleware(LoggingMiddleware(logger=QUERY_LOG))   # 每次调用自动一行日志

    # ------------------------- 资源：模型写 SQL 前先看这里 -------------------------
    @mcp.resource(
        "db://databases",
        name="可查询的数据库",
        description="全部数据源及其库名（catalogs）。query_db_async 的 database 参数只能从中选。",
    )
    def databases_resource() -> dict[str, list[str]]:
        return {name: db.catalogs for name, db in settings.databases.items()}

    @mcp.resource(
        "db://{database}/tables",
        name="库的表清单",
        description="列出某个库里的全部表名。database 用 db://databases 里看到的库名。",
    )
    async def tables_resource(database: str) -> list[str]:
        db_name, db, catalog = route_database(settings, database)
        return await list_tables(settings, db_name, db, catalog)

    # ------------------------- 提示词模板 -------------------------
    @mcp.prompt(
        "query-assistant",
        description="把自然语言问题转成一条安全的只读 SELECT 并提交。",
    )
    def query_assistant(question: str, database: str = "") -> str:
        return (
            f"请把下面的问题转成一条只读 SELECT 并提交查询。\n"
            f"目标库：{database or '默认库'}\n"
            f"问题：{question}\n\n"
            "要求：先读资源 db://databases 和 db://{库名}/tables 确认库和表名；"
            "只允许单条 SELECT；能聚合就不取明细。"
        )

    # ------------------------- 工具 -------------------------
    @mcp.tool
    async def query_db_async(sql: str, database: str = "", *, ctx: Context) -> ToolReply:
        """异步执行只读 SQL，立即返回查询ID。
        database 填 db://databases 资源里的库名，留空用默认库。
        用法：① 拿到 queryId；② 调 get_query_db_async_result 查状态。"""
        try:
            db_name, db, catalog = route_database(settings, database)
            safe_sql = validate(sql, dialect=db.dialect)
        except ValueError as e:
            return _fail(f"{e}\n请修正后重试。")
        try:
            query_id = await executor.submit(safe_sql, db_name, db, catalog)
        except asyncio.QueueFull:
            return _fail(f"当前排队已满（queue_size={settings.query.queue_size}），"
                         "请稍后重试，不要重复提交。")
        await ctx.info(f"查询已提交（{db_name}.{catalog}）")   # 实时消息直达客户端
        return ToolReply(
            success=True,
            data=query_id,
            message=f"查询已提交，queryId={query_id}。"
                    "稍后调用 get_query_db_async_result 传入该 queryId 获取结果。")

    @mcp.tool
    async def get_query_db_async_result(query_id: str, *, ctx: Context) -> ToolReply:
        """按 queryId 查询任务状态。RUNNING→等几秒再查（不要重复提交）；
        FAILED→附原因；SUCCESS→返回结果文件。"""
        task = executor.get((query_id or "").strip())
        if task is None:
            return _fail(f"查询 {query_id} 不存在或已过期，请重新提交。")
        if task["status"] == "RUNNING":
            waited = int((time.time() - task["time"]) * 1000)
            await ctx.info(f"仍在执行（已提交 {waited} ms）")
            return _fail(f"查询仍在执行（已提交 {waited} ms），请稍后重试，不要重复提交。")
        if task["status"] == "FAILED":
            return _fail(f"查询失败：{task['error']}\n请修正 SQL 后重新提交。")
        # 注意：行数刻意不回传！行数只进服务端日志（模型知道行数就会纠结要不要
        # 拉全量明细，而这件事应该在写 SQL 时决定）
        file_or_url = f"{download_base}/{task['file']}" if is_http else task["file"]
        return ToolReply(success=True, data=file_or_url, message="查询完成，结果文件已生成。")

    # ------------------------- http 模式：结果文件下载端点 -------------------------
    if is_http:
        @mcp.custom_route("/results/{filename}", methods=["GET"])
        async def download_result(request):
            filename = request.path_params["filename"]
            if not SAFE_FILENAME.fullmatch(filename):
                return PlainTextResponse("非法文件名", status_code=400)
            path = executor.results_dir / filename
            if not path.is_file():
                return PlainTextResponse("结果文件不存在或已过期", status_code=404)
            return FileResponse(path, media_type="text/csv")

    return mcp


def _setup_logging(settings: Settings) -> None:
    # stdio 模式下 stdout 属于 MCP 协议通道，日志一律走 stderr
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )


def main() -> None:
    settings = Settings()   # 加载失败（缺环境变量/校验不过）在这里就崩，越早越好
    _setup_logging(settings)
    mcp = build(settings)
    if settings.server.transport == "http":
        mcp.run(transport="http", host=settings.server.host, port=settings.server.port)
    else:
        mcp.run()   # stdio


if __name__ == "__main__":
    main()
