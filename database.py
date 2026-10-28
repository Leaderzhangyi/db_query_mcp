"""数据访问层：库名路由 + 异步连接池。

全链路 asyncio（FastMCP 的 stdio/http 传输都跑在事件循环上），
连接池用 SQLAlchemy 的异步引擎（AsyncAdaptedQueuePool），一个
create_async_engine 配齐全部语义：
  maximum-pool-size  → pool_size（max_overflow=0：固定池，上限即池大小）
  connection-timeout → pool_timeout（借出等待超时；池满抛 TimeoutError 让模型重试）
  max-lifetime       → pool_recycle（连接最大寿命，到寿自动重建）
  pool_pre_ping=True → 借出前先 ping，死连接当场换新，空闲保活一并覆盖
驱动：mysql → aiomysql，sqlite → aiosqlite（本地开发自测用）。
实例可用自己的 maximum-pool-size 覆盖池上限。

借还规则即 SQLAlchemy 语义：with 正常退出归还复用；抛异常连接作废不回池。
查询走 run_sync + stream_results 服务端游标：原始 SQL 直达驱动（不经过
text() 的 :绑定参数 解析），逐行拉取，内存与行数无关。
"""
from __future__ import annotations

import random
import re
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import quote_plus

from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from settings import BASE_DIR, InstanceSettings, Settings

_engines: dict[str, AsyncEngine] = {}
_engines_lock = threading.Lock()


def route(settings: Settings, database: str) -> tuple[str, InstanceSettings, str | None]:
    """库路由：database 是配置里某个实例的 catalog 名（忽略大小写）。

    留空 → 第一个实例的第一个库（本地演示的默认库）。
    找不到 → 报错并列出全部可选库，让模型自己改。
    返回的 catalog 保持配置里的原始大小写（USE 语句要用）。
    """
    instances = settings.mcp.db.instances
    if not instances:
        raise ValueError("未配置任何数据源实例（mcp.db.instances）")
    if not (database or "").strip():
        name = next(iter(instances))
        ins = instances[name]
        return name, ins, (ins.catalogs[0] if ins.catalogs else None)

    wanted = database.strip().lower()
    for name, ins in instances.items():
        for catalog in ins.catalogs:
            if catalog.lower() == wanted:
                return name, ins, catalog
    choices = ", ".join(sorted(c for i in instances.values() for c in i.catalogs)) or "（无）"
    raise ValueError(f"未知数据库 {database!r}，可用的库：{choices}")


@asynccontextmanager
async def borrow(settings: Settings, instance_name: str,
                 instance: InstanceSettings, catalog: str | None) -> AsyncIterator[AsyncConnection]:
    """借连接的异步上下文管理器。执行查询用 conn.run_sync(...)（见 executor）。"""
    engine = _get_engine(settings, instance_name, instance)
    async with engine.connect() as conn:
        if instance.dialect == "mysql" and catalog:
            # 切库（对应 Java 版 setCatalog）；catalog 来自已校验的配置，再防一手反引号
            if not re.fullmatch(r"[0-9A-Za-z_]+", catalog):
                raise ValueError(f"库名含非法字符：{catalog!r}")
            await conn.exec_driver_sql(f"USE `{catalog}`")
        yield conn


def _get_engine(settings: Settings, instance_name: str,
                instance: InstanceSettings) -> AsyncEngine:
    """每实例一个引擎（= 一个连接池），首次使用才创建（懒建池）。"""
    engine = _engines.get(instance_name)
    if engine is not None:
        return engine
    with _engines_lock:
        engine = _engines.get(instance_name)
        if engine is None:
            pool_cfg = settings.mcp.db.pool
            try:
                engine = create_async_engine(
                    _engine_url(instance),
                    pool_size=instance.maximum_pool_size or pool_cfg.maximum_pool_size,
                    max_overflow=0,                                # 固定池：上限即 pool_size
                    pool_timeout=pool_cfg.connection_timeout / 1000,
                    pool_recycle=pool_cfg.max_lifetime / 1000,
                    pool_pre_ping=True,
                    connect_args=({"connect_timeout": pool_cfg.connection_timeout // 1000}
                                  if instance.dialect == "mysql" else {}),
                )
            except ModuleNotFoundError as e:
                raise RuntimeError(
                    f"{instance.dialect} 实例需要异步驱动："
                    f"pip install {'aiomysql' if instance.dialect == 'mysql' else 'aiosqlite'}"
                    f"（{e}）") from e
            _engines[instance_name] = engine
        return engine


def _engine_url(instance: InstanceSettings) -> str:
    """JDBC URL → SQLAlchemy 异步 URL：实例配置保持与 Java 版同一份，两边通用。"""
    if instance.dialect == "sqlite":
        # jdbc:sqlite:demo.db → demo.db（按第 3 个冒号切）；相对路径锚定项目目录
        path = Path(instance.url.split(":", 2)[2] or "demo.db")
        if not path.is_absolute():
            path = BASE_DIR / path
        return f"sqlite+aiosqlite:///{path.as_posix()}"

    if instance.dialect == "mysql":
        # SQLAlchemy URL 不支持逗号多主机（loadbalance）；建引擎时随机选一台。
        # 只读查询对会话粘性不敏感；要多机分担就配多个实例。
        host, port = random.choice(_mysql_hosts(instance.url))
        auth = ""
        if instance.username:
            auth = f"{quote_plus(instance.username)}:{quote_plus(instance.password)}@"
        return f"mysql+aiomysql://{auth}{host}:{port}/?charset=utf8mb4"
        # charset 必须显式：默认 latin1，中文必乱码

    raise RuntimeError(f"方言 {instance.dialect!r} 暂未实现（当前支持 sqlite / mysql）")


def _mysql_hosts(url: str) -> list[tuple[str, int]]:
    """JDBC URL → [(host, port), ...]。

    jdbc:mysql://h:p            → [(h, p)]
    jdbc:mysql:loadbalance://h1:p1,h2:p2,h3:p3 → [(h1,p1), (h2,p2), (h3,p3)]
    """
    body = url.split("//", 1)[-1].split("?", 1)[0].split("/", 1)[0]
    hosts = []
    for part in body.split(","):
        host, _, port = part.partition(":")
        if host:
            hosts.append((host, int(port or 3306)))
    return hosts or [("127.0.0.1", 3306)]
