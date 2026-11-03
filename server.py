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
  ontology.py   语义层（业务概念→表字段口径，config/ontology.yaml）
  server.py     本文件：装配 + 入口

运行：python server.py              （MCP_ENV 未设 = dev：http，监听 9000）
      MCP_ENV=stdio python server.py（stdio：被 MCP 客户端拉起，见 config.stdio.yaml）
      MCP_ENV=prod python server.py （prod：银行 mysql + http）
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.server.middleware.logging import LoggingMiddleware
from pydantic import BaseModel, Field
from starlette.responses import FileResponse, PlainTextResponse

from database import list_tables, route as route_database
from executor import QueryExecutor
from ontology import (
    BrowseReply,
    MappingCard,
    OntologyStore,
    ResolveReply,
    TermCard,
    load_ontology,
)
from settings import BASE_DIR, Settings
from sql_guard import validate
from trajectory import TrajectoryRecorder, build_recorder

QUERY_LOG = logging.getLogger("QUERY_LOG")
# 结果文件名固定是 uuid.csv，白名单外的名字一律拒绝——防路径穿越
SAFE_FILENAME = re.compile(r"^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\.csv$")


class InlineTable(BaseModel):
    """小结果（行数 ≤ query.inline_max_rows）直接内联展示。"""

    columns: list[str]
    rows: list[list[str | int | float | None]]


class ToolReply(BaseModel):
    """ 所有工具的统一返回。

    data：queryId / 文件名 / 下载链接（字符串）
    table：小结果内联表（与 data 互斥——内联时不落文件）
    """
    success: bool
    message: str = ""
    data: str | None = None
    table: InlineTable | None = None


class TrajSummary(BaseModel):
    """export_trajectory 返回的一条轨迹概览。"""

    task_id: str
    database: str = ""
    status: str = ""
    rows: int = 0
    sql: str = ""          # 截断到前 80 字符，完整 SQL 在轨迹文件里


class TrajectoryReply(BaseModel):
    """export_trajectory 的返回。"""

    success: bool
    message: str = ""
    count: int = 0
    dir: str = ""                              # 轨迹落盘目录（绝对路径）
    trajectories: list[TrajSummary] = Field(default_factory=list)


def _fail(message: str) -> ToolReply:
    return ToolReply(success=False, message=message)


def build(settings: Settings | None = None, executor: QueryExecutor | None = None) -> FastMCP:
    """装配出完整的 FastMCP 实例（冒烟测试也走这里）。"""
    settings = settings or Settings()
    ontology = load_ontology(settings)   # 文件缺失=空语义层；内容不合法在这里就崩
    recorder = build_recorder(settings, ontology.version)
    executor = executor or QueryExecutor(settings, recorder=recorder)
    is_http = settings.server.transport == "http"
    download_base = settings.results.download_base_url.rstrip("/")

    mcp = FastMCP(
        settings.server.name,
        instructions=(
            "只读数据库查询服务。用法：① 读资源 db://databases 查看可用的库；"
            "② 需要表名时读 db://{库名}/tables；③ 调 query_db_async 提交 SQL，"
            "拿到 queryId；④ 调 get_query_db_async_result 轮询取结果。"
            "结果策略：小结果（约 5 行以内）直接在回复里展示；更大的落成 CSV 文件"
            "给下载链接。重要：能用 COUNT / SUM / GROUP BY 在 SQL 里算完的，"
            "就不要取全量明细。"
            "涉及业务指标（金额、数量、比率类）：先读资源 db://terms 看概念清单，"
            "调 browse_terms 找概念、resolve_term 拿口径（聚合方式/过滤条件/粒度），"
            "再按口径写 SQL——口径对了 SQL 才对。"
        ),
    )
    mcp.add_middleware(LoggingMiddleware(logger=QUERY_LOG))   # 每次调用自动一行日志

    # ------------------------- 资源：模型写 SQL 前先看这里 -------------------------
    @mcp.resource(
        "db://databases",
        name="可查询的数据库",
        description="全部数据环境（数据源）及其库名（catalogs）。"
                    "query_db_async 的 database 参数填环境名或库名。",
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

    @mcp.resource(
        "db://terms",
        name="业务概念清单",
        description="语义层登记的业务概念及其别名。写 SQL 前先 browse_terms/resolve_term 查口径。",
    )
    def terms_resource() -> list[str]:
        if not ontology:
            return ["（语义层未配置：缺 config/ontology.yaml，请直接按表名写 SQL）"]
        return [f"{t.name}（别名：{'、'.join(t.aliases) if t.aliases else '无'}）"
                for t in ontology.all_terms()]

    # ------------------------- 提示词模板 -------------------------
    def _catalog_menu() -> str:
        """当前数据源与库清单（渲染进提示词，让用户直接挑）。"""
        return "\n".join(
            f"- {name}（{db.dialect}）：{'、'.join(db.catalogs)}"
            for name, db in settings.databases.items())

    def _term_menu() -> str:
        """语义层概念清单（两段式提示词的第一段：先对齐概念再写 SQL）。"""
        if not ontology:
            return "（未配置语义层，请按表名直接分析）"
        return "、".join(ontology.term_names)

    @mcp.prompt(
        "query-assistant",
        description="把自然语言问题转成一条安全的只读 SELECT 并提交；"
                    "会附上当前可用的库，选了库还附该库的表清单。",
    )
    async def query_assistant(
        question: str,
        database: Annotated[str, Field(
            description="目标库名，留空则先列出库清单让用户选择")] = "",
    ) -> str:
        # 没选库：把库清单放进提示词，让模型先请用户挑一个再继续
        if not database.strip():
            return (
                f"用户的问题：{question}\n\n"
                f"当前可用的库：\n{_catalog_menu()}\n\n"
                f"已登记的业务概念：{_term_menu()}\n\n"
                "用户还没选库。请先把上面的库清单展示给用户让其选择，"
                "确认库名后再把问题转成查询。"
            )
        # 选了库：库名必须真实存在，顺带把该库的表清单也附上
        try:
            db_name, db, catalog = route_database(settings, database)
            tables = await list_tables(settings, db_name, db, catalog)
        except ValueError as e:
            return (
                f"用户的问题：{question}\n\n"
                f"用户填的库名不可用（{e}）。\n\n"
                f"当前可用的库：\n{_catalog_menu()}\n\n"
                "请把这份清单展示给用户重新选择。"
            )
        return (
            f"用户的问题：{question}\n\n"
            f"目标库：{database}，该库的表：{'、'.join(tables)}\n\n"
            f"已登记的业务概念：{_term_menu()}\n\n"
            "两步走：① 问题若涉及上面的业务概念，先调 resolve_term 拿口径"
            "（表、字段、聚合方式、过滤条件、粒度）；② 按口径把问题转成一条只读"
            " SELECT，调用 query_db_async 提交（database 参数填上面的库名），"
            "再用 get_query_db_async_result 取结果。"
            "要求：只允许单条 SELECT；能聚合就不取明细；口径里的 filter 必须体现进 WHERE。"
        )

    # ------------------------- 工具 -------------------------
    @mcp.tool
    async def query_db_async(sql: str, database: str = "", *, ctx: Context) -> ToolReply:
        """异步执行只读 SQL，立即返回查询ID。
        database 填 db://databases 资源里的环境名（如 dstg2）或具体库名，
        留空用默认环境。
        SQL 会先过语法守卫（只读单条 SELECT）再过语义约束检查
        （敏感表/口径提醒，命中 block 会被拦截并附替代方案）。
        结果策略：小结果（约 5 行以内）会在 get_query_db_async_result 里直接展示；
        更大的落成 CSV 文件给下载链接。
        用法：① 拿到 queryId；② 调 get_query_db_async_result 查状态。"""
        try:
            db_name, db, catalog = route_database(settings, database)
            safe_sql = validate(sql, dialect=db.dialect)
        except ValueError as e:
            return _fail(f"{e}\n请修正后重试。")
        # 语义约束：语法守卫管「安不安全」，这里管「口径对不对」——
        # block 拦下并给替代方案；warn 放行但随回执带上口径提示
        blocks, warns = ontology.check_sql(safe_sql, dialect=db.dialect)
        if blocks:
            QUERY_LOG.warning("语义拦截 database=%s sql=%.80s 原因=%s",
                              database, sql, "；".join(blocks))
            return _fail(f"语义约束拦截：{'；'.join(blocks)}\n请按提示调整 SQL。")
        try:
            query_id = await executor.submit(safe_sql, db_name, db, catalog)
        except asyncio.QueueFull:
            return _fail(f"当前排队已满（queue_size={settings.query.queue_size}），"
                         "请稍后重试，不要重复提交。")
        await ctx.info(f"查询已提交（{db_name}.{catalog}）")   # 实时消息直达客户端
        warn_text = f"\n口径提示：{'；'.join(warns)}" if warns else ""
        return ToolReply(
            success=True,
            data=query_id,
            message=f"查询已提交，queryId={query_id}。"
                    "稍后调用 get_query_db_async_result 传入该 queryId 获取结果。" + warn_text)

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
        inline = task.get("table")
        if inline:
            # 小结果：直接把表放在回复里，模型当场展示给用户，不用下载
            return ToolReply(
                success=True, table=inline,
                message=f"查询完成，共 {len(inline['rows'])} 行，结果如下。")
        # 大结果：只给文件/下载链接。行数刻意不回传——行数只进服务端日志，
        # 模型不知道行数就不会纠结要不要拉全量明细（该在写 SQL 时决定）
        file_or_url = f"{download_base}/{task['file']}" if is_http else task["file"]
        return ToolReply(success=True, data=file_or_url, message="查询完成，结果已保存为 CSV 文件。")

    # ------------------------- 工具：语义层（口径查询） -------------------------
    @mcp.tool
    async def browse_terms(query: str) -> BrowseReply:
        """按自然语言找业务概念（如"已支付金额""下单量""客户数"）。
        返回匹配概念的 id/名字/定义；选定后调 resolve_term 拿完整口径再写 SQL。
        语义层未配置时 terms 为空、message 说明原因。"""
        if not ontology:
            return BrowseReply(
                success=True, terms=[],
                message="语义层未配置（缺 config/ontology.yaml），请直接按表名写 SQL。")
        hits = ontology.browse(query)
        if not hits:
            return BrowseReply(
                success=True, terms=[],
                message=f"没有匹配的概念。已登记：{'、'.join(ontology.term_names)}")
        return BrowseReply(
            success=True, terms=[TermCard(id=t.id, name=t.name, definition=t.definition) for t in hits],
            message=f"匹配到 {len(hits)} 个概念，调 resolve_term（传名字或 id）获取口径。")

    @mcp.tool
    async def resolve_term(term: str) -> ResolveReply:
        """概念名/别名/id → 完整口径：落到哪个库哪个表哪个字段、怎么聚合
        （aggregation）、业务过滤（filter）、粒度（grain），以及关联概念。
        写 SQL 前先调这里——口径对了 SQL 才对。"""
        hit = ontology.resolve(term)
        if hit is None:
            hint = "、".join(ontology.term_names) or "（语义层未配置）"
            return ResolveReply(success=False,
                                message=f"未登记的概念 {term!r}。已登记：{hint}")
        return ResolveReply(
            success=True, id=hit.id, name=hit.name, definition=hit.definition,
            aliases=hit.aliases,
            mappings=[MappingCard(**m.model_dump()) for m in hit.mappings],
            relations=ontology.relations_of(hit),
            constraints=ontology.constraints_of(hit),
            message="口径如上。按 mappings 定位表字段，filter 拼进 WHERE，"
                    "grain 提醒你一行是什么（决定要不要 DISTINCT）；"
                    "constraints 是该表已有的语义约束，block 级的别碰。")

    @mcp.tool
    async def export_trajectory(limit: int = 20) -> TrajectoryReply:
        """导出最近 limit 条查询轨迹概览（EvoOntology rolling_trajectory 格式）。
        每次 query_db_async 完成都会自动落一条轨迹 JSON 到 trajectories/ 目录，
        供 /evoontology:evolve-ontology 等外部进化流程消费。
        本工具返回轨迹概览（task_id/状态/SQL 摘要），完整内容在返回的 dir 目录里。"""
        summaries = recorder.summaries(limit)
        return TrajectoryReply(
            success=True,
            count=len(summaries),
            dir=str(recorder.dir),
            trajectories=[
                TrajSummary(
                    task_id=s["task_id"], database=s["database"], status=s["status"],
                    rows=s["rows"], sql=s["sql"][:80],
                )
                for s in summaries
            ],
            message=(f"共 {len(summaries)} 条轨迹，完整 JSON 在 {recorder.dir}；"
                     "每条 trajectory 含 task_id/question/ontology_version/"
                     "native_tool_calls/task_status，可直接喂给 EvoOntology 的"
                     " rolling_trajectory 进化流程（question 字段可由客户端补）。"),
        )

    # ------------------------- http 模式：结果文件下载端点 -------------------------
    # 注意：FastMCP 的 LoggingMiddleware 只覆盖 MCP 协议调用（工具/资源/提示词），
    # 自定义 HTTP 路由不经过它，下载日志在这里自己记。
    if is_http:
        @mcp.custom_route("/results/{filename}", methods=["GET"])
        async def download_result(request):
            filename = request.path_params["filename"]
            client = request.client.host if request.client else "?"
            if not SAFE_FILENAME.fullmatch(filename):
                QUERY_LOG.warning("下载拒绝 来源=%s 文件名=%r 原因=非法文件名", client, filename)
                return PlainTextResponse("非法文件名", status_code=400)
            path = executor.results_dir / filename
            if not path.is_file():
                QUERY_LOG.warning("下载未命中 来源=%s 文件=%s 原因=不存在或已过期", client, filename)
                return PlainTextResponse("结果文件不存在或已过期", status_code=404)
            QUERY_LOG.info("下载 来源=%s 文件=%s 大小=%dB", client, filename, path.stat().st_size)
            return FileResponse(path, media_type="text/csv")

    return mcp


def _setup_logging(settings: Settings) -> None:
    """三路输出：stderr 控制台 + logs/app.log（全部）+ logs/error.log（WARNING+）。

    目录不存在自动创建；文件按大小轮转，不用引第三方库。
    force=True 保证重复调用（测试里）也能拿到干净的 root handlers。
    """
    cfg = settings.logging
    logs_dir = Path(cfg.dir)
    if not logs_dir.is_absolute():
        logs_dir = BASE_DIR / logs_dir
    logs_dir.mkdir(parents=True, exist_ok=True)     # ← 没有目录就建

    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    console = logging.StreamHandler(sys.stderr)     # stdio 模式 stdout 属于协议通道，只能走 stderr
    app_file = RotatingFileHandler(logs_dir / "app.log", maxBytes=cfg.max_bytes,
                                   backupCount=cfg.backup_count, encoding="utf-8")
    err_file = RotatingFileHandler(logs_dir / "error.log", maxBytes=cfg.max_bytes,
                                   backupCount=cfg.backup_count, encoding="utf-8")
    err_file.setLevel(logging.WARNING)              # 这个文件只收 WARNING 及以上
    for handler in (console, app_file, err_file):
        handler.setFormatter(formatter)
    logging.basicConfig(level=cfg.level.upper(), force=True,
                        handlers=[console, app_file, err_file])


def _log_startup(settings: Settings) -> None:
    """启动日志：一眼确认配置是否按预期生效（传输 / 端口 / 数据源 / 池参数）。"""
    sources = ", ".join(
        f"{name}[{db.dialect}]({'/'.join(db.catalogs)})"
        for name, db in settings.databases.items())
    if settings.server.transport == "http":
        QUERY_LOG.info(
            "启动 %s：http://%s:%d 下载根=%s 连接池上限=%s 数据源：%s",
            settings.server.name, settings.server.host, settings.server.port,
            settings.results.download_base_url,
            "/".join(str(db.pool_size or settings.pool.size)
                     for db in settings.databases.values()),
            sources)
    else:
        QUERY_LOG.info("启动 %s：stdio 数据源：%s", settings.server.name, sources)


def main() -> None:
    settings = Settings()   # 加载失败（缺环境变量/校验不过）在这里就崩，越早越好
    _setup_logging(settings)
    _log_startup(settings)
    mcp = build(settings)
    if settings.server.transport == "http":
        mcp.run(transport="http", host=settings.server.host, port=settings.server.port)
    else:
        mcp.run()   # stdio


if __name__ == "__main__":
    main()
