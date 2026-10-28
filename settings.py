"""配置层：pydantic-settings 读取 config/config.yaml。

== 整体流程 ==

    Settings()                          ← 入口，加载失败直接抛（启动即崩，越早越好）
      │
      ├─ ① pydantic-settings 按「来源清单」收集值
      │     来源清单由 settings_customise_sources() 决定（见下方类定义处注释）：
      │       init_settings：显式传参（冒烟测试用），没传就跳过
      │       YamlSource：   读 config.yaml + config.{MCP_ENV}.yaml
      │                      → 展开 ${VAR} 占位符 → 深合并 → 返回整棵树 dict
      │
      ├─ ② pydantic 校验：dict → 嵌套模型（类型转换、必填检查、别名匹配）
      │
      └─ ③ Settings._startup_checks：跨字段的启动校验，不过关就拒绝启动

== 环境切换（dev / prod）==

    MCP_ENV 不设置（默认 dev）→ 深合并 config.dev.yaml   （本地：stdio + SQLite）
    MCP_ENV=prod               → 深合并 config.prod.yaml （生产：http + MySQL）
    覆盖层文件不存在就当空处理（只用主配置）。

== 配置文件的值支持占位符 ==

    ${MYSQL_USER}       → 取环境变量 MYSQL_USER；未设置且无默认值 → 启动即报错
    ${MYSQL_USER:root}  → 未设置时用默认值 root
    敏感信息（密码等）不落盘，只写占位符，实际值从环境变量注入。
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

BASE_DIR = Path(__file__).parent                    # 项目根目录（config/results 都锚定在这）
CONFIG_FILE = BASE_DIR / "config" / "config.yaml"   # 主配置；覆盖层在同目录

# 匹配 ${VAR} 或 ${VAR:默认值}；\w+ 是变量名，冒号后到 } 前是默认值（可有可无）
_PLACEHOLDER = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")

SUPPORTED_DIALECTS = ("sqlite", "mysql")


# ============================================================================
# 以下是配置树的各个「段」：每段一个 BaseModel，字段名 = yaml 键名。
# pydantic 负责类型校验和默认值——写错类型、漏填必填项，启动时就会报错，
# 而不是等到运行到一半才炸。
# ============================================================================


class ServerSettings(BaseModel):
    """server 段：服务器自身的信息。"""
    name: str = "db-query-server"     # MCP 服务器名（客户端列表里显示的名字）
    transport: str = "stdio"          # stdio：被 MCP 客户端拉起（本地开发）
                                      # http ：独立部署，监听下面的 host:port
    host: str = "0.0.0.0"             # http 模式监听地址
    port: int = 9001                  # http 模式监听端口


class PoolSettings(BaseModel):
    """pool 段：连接池默认值（每个数据源一个池，见 database.py）。

    这些值最终传给 SQLAlchemy 的 create_async_engine：
      size            → pool_size（配 max_overflow=0，固定池：上限即池大小）
      connect_timeout_ms → pool_timeout（借连接的等待超时；池满抛错让调用方重试）
      max_lifetime_s  → pool_recycle（连接最长活多久，到寿自动重建，
                        避免用到被网络设备悄悄掐断的旧连接）
      pre_ping        → pool_pre_ping（借出前先 ping 一下，死连接当场换新）
    """

    size: int = 5                     # 池上限；单个数据源可用自己的 pool_size 覆盖
    connect_timeout_ms: int = 3000
    max_lifetime_s: int = 1740        # 29 分钟，比常见防火墙的空闲超时略短
    pre_ping: bool = True


class QuerySettings(BaseModel):
    """query 段：查询执行（asyncio 任务池 + 任务暂存区）。

    全链路跑在单个事件循环上，没有线程和锁——所以这里没有
    「核心线程数/存活时间」这类线程池概念，只有并发数和队列上限。
    """

    workers: int = 20                 # 同时执行的查询数（工作协程数）
    queue_size: int = 64              # 排队上限；满了直接拒绝提交（报错让模型稍后重试），
                                      # 绝不让请求无限堆积拖垮内存
    task_ttl_minutes: int = 30        # 任务记录保留时长；过期后查询状态按「不存在」处理
    max_tasks: int = 200              # 内存中最多保留的任务数；超出按「最久未访问」淘汰，
                                      # 正在执行（RUNNING）的不会被淘汰


class ResultsSettings(BaseModel):
    """results 段：查询结果落盘与下载。"""

    dir: str = "results"              # CSV 结果文件的目录（相对项目根目录）
    ttl_minutes: int = 40             # 结果文件保留时长。必须大于 query.task_ttl_minutes
                                      # ——否则内存里任务还在、磁盘上文件已被删（见启动校验）
    max_total_bytes: int = 10737418240    # 结果目录总容量兜底（10GB）；正常靠 TTL 回收，
                                          # 这里只兜「TTL 窗口内堆积出海量查询」的极端情况
    download_base_url: str = ""       # http 模式必填：Agent 下载文件用的对外地址。
                                      # 配不出一个真能下载的地址就不该启动（见启动校验）


class DatabaseSettings(BaseModel):
    """databases 段里的一个数据源：一套连接，下挂多个库（catalogs）。

    一个数据源 = 一个连接池；「库」不在连接上写死，查询时按 catalog 路由
    （mysql 用 USE 切库，sqlite 一个文件就是一个库）。

    sqlite 只需要 path；mysql 需要 hosts + 账号。缺什么由启动校验兜底。
    """

    dialect: str                      # sqlite | mysql（必填，不猜、不省略）
    catalogs: list[str] = Field(default_factory=list)
                                      # 这个数据源里的库名；查询的 database 参数只能从中选
    # ---- sqlite 专用 ----
    path: str = ""                    # 数据库文件路径；相对路径按项目根目录解析
    # ---- mysql 专用 ----
    hosts: list[str] = Field(default_factory=list)
                                      # 等价主机组（"ip:端口"）；新建连接时随机挑一台，
                                      # 只读查询对连哪台不敏感
    username: str = ""
    password: str = ""                # 建议在 yaml 里写 ${MYSQL_PASSWORD} 占位符
    pool_size: int | None = None      # 覆盖全局 pool.size；None = 用全局值


# ============================================================================
# Settings：整棵配置树。BaseSettings 与普通 BaseModel 的区别在于它有
# 「来源清单」机制——值可以从多个来源按优先级收集，而不只是 __init__ 传参。
# ============================================================================


class Settings(BaseSettings):
    """整棵配置树。入口：Settings()——加载失败（缺环境变量/校验不过）直接抛。"""

    # extra="ignore"：yaml 里出现模型没有的键时不报错（方便写注释性配置、向前兼容）
    model_config = SettingsConfigDict(extra="ignore")

    server: ServerSettings = Field(default_factory=ServerSettings)
    pool: PoolSettings = Field(default_factory=PoolSettings)
    query: QuerySettings = Field(default_factory=QuerySettings)
    results: ResultsSettings = Field(default_factory=ResultsSettings)
    databases: dict[str, DatabaseSettings] = Field(default_factory=dict)
    log_level: str = "INFO"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,      # 显式传参（优先级最高）
        env_settings: PydanticBaseSettingsSource,       # 环境变量（默认启用，这里被我们丢弃）
        dotenv_settings: PydanticBaseSettingsSource,    # .env 文件（默认启用，这里被我们丢弃）
        file_secret_settings: PydanticBaseSettingsSource,  # secrets 目录（默认启用，这里被我们丢弃）
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """决定「配置值从哪来」：返回的元组就是来源清单，顺序 = 优先级（前面高）。

        为什么必须写这个钩子？
          ① BaseSettings 默认的来源是 init传参 > 环境变量 > .env > secrets，
            清单里根本没有 yaml——不装自定义源，Settings() 永远不会读 config.yaml。
          ② 光在 model_config 里写 yaml_file 也会被忽略（pydantic-settings 2.x
            会直接告警），装 yaml 源的唯一正式入口就是这个钩子。
          ③ 我们要的不止「读一个 yaml」：还要按 MCP_ENV 合并覆盖层、展开 ${VAR}
            占位符——所以用自定义的 YamlSource，而不是内置的 yaml 源。

        为什么丢弃 env_settings（环境变量源）？
          默认行为下，环境变量 LOG_LEVEL=DEBUG 会悄悄覆盖 log_level 字段——
          一个同名变量就能改掉配置，出了问题很难排查。关掉之后，环境变量
          想进配置只有一条路：yaml 里显式写 ${VAR} 占位符，改了什么一目了然。
        """
        return (init_settings, YamlSource(settings_cls))

    @model_validator(mode="after")
    def _startup_checks(self) -> "Settings":
        """启动期校验：跨字段的约束在这里查，单个字段的类型/必填 pydantic 已查过。

        原则：配置错误要在启动瞬间爆出来，绝不带病上岗。
        """
        # —— 数据源必须配且配全 ——
        if not self.databases:
            raise ValueError("databases 至少要配置一个数据源")
        for name, db in self.databases.items():
            if db.dialect not in SUPPORTED_DIALECTS:
                raise ValueError(
                    f"databases.{name}.dialect={db.dialect!r} 不支持"
                    f"（可选：{'/'.join(SUPPORTED_DIALECTS)}）")
            if not db.catalogs:
                raise ValueError(f"databases.{name}.catalogs 不能为空")
            # 各方言的必填项：sqlite 要 path，mysql 要 hosts
            if db.dialect == "sqlite" and not db.path:
                raise ValueError(f"databases.{name} 是 sqlite，必须配置 path")
            if db.dialect == "mysql" and not db.hosts:
                raise ValueError(f"databases.{name} 是 mysql，必须配置 hosts")
            # hosts 里每项都应是 "地址:数字端口"
            for host in db.hosts:
                _, _, port = host.partition(":")
                if not port.isdigit():
                    raise ValueError(f"databases.{name}.hosts 里 {host!r} 端口不是数字")

        # —— 结果文件要比任务记录活得久 ——
        # 任务 TTL 30 分钟、文件 TTL 40 分钟：模型 29 分钟时还能查到「成功」，
        # 若文件 20 分钟就被删，模型会拿到一个下载不了的链接
        if self.results.ttl_minutes <= self.query.task_ttl_minutes:
            raise ValueError(
                f"results.ttl-minutes({self.results.ttl_minutes}) 必须大于 "
                f"query.task-ttl-minutes({self.query.task_ttl_minutes})，"
                "否则内存里任务还在、磁盘上结果已被删")

        # —— http 模式必须给下载地址 ——
        # http 模式下 SUCCESS 返回的是下载链接；地址配错或没配，
        # 模型会拿到一个永远下载不了的 URL，比启动失败更难排查
        if self.server.transport == "http" and not self.results.download_base_url:
            raise ValueError("transport=http 时 results.download_base_url 必填"
                             "（在 config.{env}.yaml 里按部署环境提供）")
        return self


# ============================================================================
# 以下是 YamlSource 的两块工具：占位符展开 + 深合并。
# ============================================================================


def _expand_placeholders(text: str, source: Path) -> str:
    """在 yaml 原文上展开 ${VAR} / ${VAR:默认值}。

    举例（config.prod.yaml 里）：
        username: ${MYSQL_USER}           → 环境变量 MYSQL_USER 的值；没设置就报错
        download-base-url: ${BASE:x}      → 没设置 BASE 时用 "x"
    报错信息带上文件名和变量名，部署时一眼知道缺哪个环境变量。
    注意：是在 safe_load 之前的纯文本上做替换，所以注释里的 ${...} 也会被
    展开——配置文件注释里别写字面量占位符示例（踩过的坑）。
    """

    def _sub(m: re.Match[str]) -> str:
        name, default = m.group(1), m.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ValueError(
            f"{source.name} 引用的环境变量 {{{name}}} 未设置："
            f"请启动前设置它，或改用 {{{name}:默认值}} 写法")

    return _PLACEHOLDER.sub(_sub, text)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """覆盖层深合并：dict 按键合并，其余类型整体覆盖；返回新 dict，不改入参。

        base    = {server: {port: 9001, name: "a"}, databases: {demo: {...}}}
        override = {server: {port: 19001}}
        结果     = {server: {port: 19001, name: "a"}, databases: {demo: {...}}}
                                        ↑ 只覆盖同名键，其余原样保留
    这就是 config.dev.yaml / config.prod.yaml 能「只写差异」的原因。
    """
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


class YamlSource(PydanticBaseSettingsSource):
    """自定义配置源：config.yaml + MCP_ENV 覆盖层 + 占位符展开，整树返回。

    pydantic-settings 对「来源」的约定非常简单：它是一个可调用对象，
    __call__() 返回一个 {字段名: 值} 的 dict（顶层键要和 Settings 的字段名
    对上），剩下的类型转换和嵌套校验交给 pydantic。所以我们的全部个性化
    逻辑——选环境、展开占位符、深合并——都收在 __call__ 里。
    """

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        # 这是基类的抽象方法（逐字段取值的通道），但我们的源整树返回、
        # 不走这条通道，所以只留一个「不可到达」的实现
        raise NotImplementedError  # 整树经 __call__ 返回，不走逐字段通道

    def __call__(self) -> dict[str, Any]:
        # ① 主配置；② 按 MCP_ENV 读覆盖层（默认 dev）；③ 深合并后整树返回
        base = self._load(CONFIG_FILE)
        env = os.environ.get("MCP_ENV", "dev")
        overlay = self._load(CONFIG_FILE.with_name(f"config.{env}.yaml"))
        return _deep_merge(base, overlay)

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        # 文件不存在 → 空覆盖层（只用主配置），不报错
        if not path.exists():
            return {}
        # 先在纯文本上展开 ${VAR}，再交给 yaml 解析
        text = _expand_placeholders(path.read_text(encoding="utf-8"), path)
        return yaml.safe_load(text) or {}
