"""数据访问层：库路由 + 异步连接池 + 元数据查询。

连接池用 SQLAlchemy 异步引擎（mysql → aiomysql，sqlite → aiosqlite），
一个 create_async_engine 配齐全部语义：
  pool.size / database.pool_size → pool_size（max_overflow=0：固定池）
  pool.connect_timeout_ms        → pool_timeout（借出等待超时，池满抛错）
  pool.max_lifetime_s            → pool_recycle（到寿自动重建）
  pool.pre_ping                  → 借出前 ping，死连接当场换新

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

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from settings import BASE_DIR, DatabaseSettings, Settings

_engines: dict[str, AsyncEngine] = {}
_engines_lock = threading.Lock()

_SAFE_IDENT = re.compile(r"^[0-9A-Za-z_]+$")


def route(settings: Settings, database: str) -> tuple[str, DatabaseSettings, str | None]:
    """库路由：database 是配置里某个数据源的 catalog 名（忽略大小写）。

    留空 → 第一个数据源的第一个库（默认库）。
    找不到 → 报错并列出全部可选库，让模型自己改。
    返回的 catalog 保持配置里的原始大小写（USE 语句要用）。
    """
    databases = settings.databases
    if not databases:
        raise ValueError("未配置任何数据源（databases）")
    if not (database or "").strip():
        name = next(iter(databases))
        db = databases[name]
        return name, db, (db.catalogs[0] if db.catalogs else None)

    wanted = database.strip().lower()
    for name, db in databases.items():
        for catalog in db.catalogs:
            if catalog.lower() == wanted:
                return name, db, catalog
    choices = ", ".join(sorted(c for d in databases.values() for c in d.catalogs)) or "（无）"
    raise ValueError(f"未知数据库 {database!r}，可用的库：{choices}")


@asynccontextmanager
async def borrow(settings: Settings, db_name: str,
                 db: DatabaseSettings, catalog: str | None) -> AsyncIterator[AsyncConnection]:
    """借连接的异步上下文管理器。执行查询用 conn.run_sync(...)（见 executor）。"""
    engine = _get_engine(settings, db_name, db)
    async with engine.connect() as conn:
        if db.dialect == "mysql" and catalog:
            if not _SAFE_IDENT.match(catalog):   # catalog 来自已校验的配置，再防一手
                raise ValueError(f"库名含非法字符：{catalog!r}")
            await conn.exec_driver_sql(f"USE `{catalog}`")
        yield conn


async def list_tables(settings: Settings, db_name: str,
                      db: DatabaseSettings, catalog: str | None) -> list[str]:
    """列出库里的表名（给 db://{database}/tables 资源用）。"""
    async with borrow(settings, db_name, db, catalog) as conn:
        if db.dialect == "sqlite":
            sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            rows = (await conn.exec_driver_sql(sql)).fetchall()
        else:
            sql = ("SELECT table_name FROM information_schema.tables "
                   f"WHERE table_schema = '{catalog}' ORDER BY table_name")
            rows = (await conn.exec_driver_sql(sql)).fetchall()
        return [row[0] for row in rows]


def _get_engine(settings: Settings, db_name: str, db: DatabaseSettings) -> AsyncEngine:
    """每数据源一个引擎（= 一个连接池），首次使用才创建（懒建池）。"""
    engine = _engines.get(db_name)
    if engine is not None:
        return engine
    with _engines_lock:
        engine = _engines.get(db_name)
        if engine is None:
            pool_cfg = settings.pool
            try:
                engine = create_async_engine(
                    _engine_url(db),
                    pool_size=db.pool_size or pool_cfg.size,
                    max_overflow=0,                                # 固定池：上限即 pool_size
                    pool_timeout=pool_cfg.connect_timeout_ms / 1000,
                    pool_recycle=pool_cfg.max_lifetime_s,
                    pool_pre_ping=pool_cfg.pre_ping,
                    connect_args=({"connect_timeout": pool_cfg.connect_timeout_ms // 1000}
                                  if db.dialect == "mysql" else {}),
                )
            except ModuleNotFoundError as e:
                raise RuntimeError(
                    f"{db.dialect} 数据源需要异步驱动："
                    f"pip install {'aiomysql' if db.dialect == 'mysql' else 'aiosqlite'}"
                    f"（{e}）") from e
            _engines[db_name] = engine
        return engine


def _engine_url(db: DatabaseSettings) -> str:
    if db.dialect == "sqlite":
        path = Path(db.path or "demo.db")
        if not path.is_absolute():
            path = BASE_DIR / path
        return f"sqlite+aiosqlite:///{path.as_posix()}"

    if db.dialect == "mysql":
        # 等价主机随机挑一台（SQLAlchemy URL 不支持多主机）；只读查询对
        # 会话粘性不敏感。要多机分担就配多个数据源。
        host = random.choice(db.hosts) if db.hosts else "127.0.0.1:3306"
        auth = ""
        if db.username:
            auth = f"{quote_plus(db.username)}:{quote_plus(db.password)}@"
        return f"mysql+aiomysql://{auth}{host}/?charset=utf8mb4"
        # charset 必须显式：默认 latin1，中文必乱码

    raise RuntimeError(f"方言 {db.dialect!r} 暂未实现（当前支持 sqlite / mysql）")
