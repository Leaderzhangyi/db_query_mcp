"""入口：配置 → 装配 → 运行。

各文件分工（一个文件一件事）：
  settings.py   配置读取（pydantic-settings 读 config/application.yml）
  database.py   数据访问（库名路由 + JDBC URL 解析 + 建连）
  executor.py   数据处理（任务表 / 工作线程 / CSV 流式落盘 / TTL 回收）
  sql_guard.py  SQL 守卫（只放行单条只读 SELECT）
  server.py     本文件：装配 + MCP 工具注册 + 运行

运行：
  python server.py                                  # dev：stdio（MCP 客户端拉起）
  SPRING_PROFILES_ACTIVE=prod python server.py      # prod：HTTP，端口 server.port
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
import time

from fastmcp import FastMCP
from starlette.responses import FileResponse, PlainTextResponse

from database import route as route_database
from executor import QueryExecutor
from settings import Settings
from sql_guard import validate

# 结果文件名固定是 uuid.csv，白名单外的名字一律拒绝——防路径穿越（../../etc/passwd）
SAFE_FILENAME = re.compile(r"^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\.csv$")


def _ok(data, message: str | None = None) -> dict:
    return {"success": True, "message": message, "data": data}


def _fail(message: str) -> dict:
    return {"success": False, "message": message, "data": None}


def build(settings: Settings | None = None, executor: QueryExecutor | None = None) -> FastMCP:
    """装配出完整的 FastMCP 实例（冒烟测试也走这里）。"""
    settings = settings or Settings()
    executor = executor or QueryExecutor(settings)
    db_cfg = settings.mcp.db
    is_http = settings.spring.ai.mcp.server.protocol == "streamable"
    download_base = db_cfg.result.download_base_url.rstrip("/")

    mcp = FastMCP(settings.spring.ai.mcp.server.name)

    @mcp.tool
    async def query_db_async(sql: str, database: str = "") -> dict:
        """异步执行只读 SQL，立即返回查询ID（JSON：success/message/data）。
        database 填配置里的库名（如 demo / sdls_des），留空用默认库。
        用法：① 拿到 queryId；② 调 get_query_db_async_result 查状态。
        重要：能用 COUNT / SUM / GROUP BY 在 SQL 里算完的，就不要取全量明细。"""
        try:
            instance_name, instance, catalog = route_database(settings, database)
            safe_sql = validate(sql, dialect=instance.dialect or "mysql")
        except ValueError as e:
            return _fail(f"{e}\n请修正后重试。")
        try:
            query_id = await executor.submit(safe_sql, instance_name, instance, catalog)
        except asyncio.QueueFull:
            return _fail(f"当前排队已满（queue-capacity={db_cfg.async_.queue_capacity}），"
                         "请稍后重试，不要重复提交。")
        return _ok(query_id, f"查询已提交，queryId={query_id}。"
                             "稍后调用 get_query_db_async_result 传入该 queryId 获取结果。")

    @mcp.tool
    async def get_query_db_async_result(query_id: str) -> dict:
        """按 queryId 查询任务状态。RUNNING→等几秒再查（不要重复提交）；
        FAILED→附原因；SUCCESS→返回结果文件（http 模式为下载链接）。"""
        task = executor.get((query_id or "").strip())
        if task is None:
            return _fail(f"查询 {query_id} 不存在或已过期，请重新提交。")
        if task["status"] == "RUNNING":
            waited = int((time.time() - task["time"]) * 1000)
            return _fail(f"查询仍在执行（已提交 {waited} ms），请稍后重试，不要重复提交。")
        if task["status"] == "FAILED":
            return _fail(f"查询失败：{task['error']}\n请修正 SQL 后重新提交。")
        # 注意：行数刻意不回传！行数只进服务端日志 QUERY_LOG（Java 版同理），
        # 模型一旦知道行数就会纠结“要不要取明细”，而这件事应该在写 SQL 时决定
        if is_http:
            return _ok(f"{download_base}/{task['file']}", "查询完成，结果文件已生成。")
        return _ok(task["file"], "查询完成，结果文件已生成。")

    if is_http:
        @mcp.custom_route("/results/{filename}", methods=["GET"])
        async def download_result(request):
            """结果文件下载端点（对应 Java 版下载接口）。"""
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
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    for name, level in settings.logging.level.items():
        logging.getLogger(name).setLevel(getattr(logging, level.upper(), logging.INFO))


def main() -> None:
    settings = Settings()   # 加载失败（缺环境变量/交叉校验不过）在这里就崩，越早越好
    _setup_logging(settings)
    mcp = build(settings)
    if settings.spring.ai.mcp.server.protocol == "streamable":
        mcp.run(transport="http", host="0.0.0.0", port=settings.server.port)
    else:
        mcp.run()   # stdio（默认 dev）


if __name__ == "__main__":
    main()
