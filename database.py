"""数据访问层：库名路由到实例 + 建立 DB-API 连接。

只做两件事，不关心 SQL 校验（sql_guard 管）也不关心调度（executor 管）：
  route()   库名 → (实例名, 实例配置, 库名)，给上层挑数据源
  connect() 实例配置 → 连接；mysql 按 JDBC URL 解析，sqlite 直接开文件
"""
from __future__ import annotations

import random
import sqlite3
from pathlib import Path
from typing import Any

from settings import BASE_DIR, InstanceSettings, Settings


def route(settings: Settings, database: str) -> tuple[str, InstanceSettings, str | None]:
    """库路由：database 是配置里某个实例的 catalog 名（忽略大小写）。

    留空 → 第一个实例的第一个库（本地演示的默认库）。
    找不到 → 报错并列出全部可选库，让模型自己改。
    返回的 catalog 保持配置里的原始大小写（select_db 要用）。
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


def connect(ins: InstanceSettings, catalog: str | None,
            connect_timeout_ms: int = 3000) -> Any:
    """按实例方言建连接。返回 DB-API 2.0 Connection，游标逐行迭代即流式。"""
    if ins.dialect == "sqlite":
        # jdbc:sqlite:demo.db → demo.db（注意按第 3 个冒号切，别切在第 1 个）；
        # 相对路径按项目目录解析（不随启动 CWD 漂移）。sqlite 的“库”就是整个
        # 文件，catalog 已在 route 阶段用掉了，这里忽略。
        path = Path(ins.url.split(":", 2)[2] or "demo.db")
        if not path.is_absolute():
            path = BASE_DIR / path
        return sqlite3.connect(path)

    if ins.dialect == "mysql":
        try:
            import pymysql
            import pymysql.cursors
        except ImportError as e:
            raise RuntimeError("MySQL 实例需要驱动：pip install pymysql") from e
        # loadbalance URL 有多台，每次连接随机挑一台——没有真实负载均衡器时的近似
        host, port = random.choice(_mysql_hosts(ins.url))
        conn = pymysql.connect(
            host=host,
            port=port,
            user=ins.username or None,
            password=ins.password or None,
            connect_timeout=connect_timeout_ms // 1000,
            # 服务端游标：逐行拉取，结果集不整包进内存（对应 Java 版流式读取）
            cursorclass=pymysql.cursors.SSCursor,
        )
        if catalog:
            conn.select_db(catalog)   # 对应 Java 版 setCatalog 动态切库
        return conn

    raise RuntimeError(f"方言 {ins.dialect!r} 暂未实现（当前支持 sqlite / mysql）")


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
