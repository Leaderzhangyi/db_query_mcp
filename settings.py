"""配置层：用 pydantic-settings 读取与 Java 版同构的 application.yml。

同一棵配置树（server / spring / logging / mcp.db.*），加载流程模拟 Spring：

  ① 读主配置 config/application.yml
  ② 按 profile 深合并 config/application-{profile}.yml（同名键覆盖、dict 按键合并）
     profile 优先级：--spring.profiles.active= > 环境变量 SPRING_PROFILES_ACTIVE > 配置文件
  ③ 展开 ${ENV_VAR} / ${ENV_VAR:默认值} 占位符，变量缺失且无默认值 → 启动即报错
  ④ pydantic 校验：yml 的 kebab-case 键自动映射到 snake_case 字段，
     并执行 Java 版同款启动期交叉校验（TTL、download-base-url、实例方言）
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_snake
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config" / "application.yml"

# 实例键名恰好是方言名时 dialect 可省略；自定义键名必须显式写（与 Java 版一致）
KNOWN_DIALECTS = ("mysql", "oracle", "postgresql", "sqlite")

_PLACEHOLDER = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")


def to_kebab(name: str) -> str:
    """字段名与 yml 键的映射：maximum_pool_size ↔ maximum-pool-size。"""
    return to_snake(name).replace("_", "-")


class YmlModel(BaseModel):
    """所有 yml 配置段的基类：接受连字符键名，未知键忽略（向前兼容真实配置）。"""

    model_config = ConfigDict(alias_generator=to_kebab, populate_by_name=True, extra="ignore")


# ------------------------- spring 段 -------------------------
class McpServerSettings(YmlModel):
    name: str = "db-query-server"
    version: str = "1.0.0"
    protocol: str = "streamable"   # streamable=HTTP / stdio
    type: str = "sync"             # 兼容保留


class SpringMcpSettings(YmlModel):
    server: McpServerSettings = Field(default_factory=McpServerSettings)


class AiSettings(YmlModel):
    mcp: SpringMcpSettings = Field(default_factory=SpringMcpSettings)


class ProfilesSettings(YmlModel):
    active: str = "dev"


class ApplicationSettings(YmlModel):
    name: str = "db-mcp-server"


class SpringSettings(YmlModel):
    profiles: ProfilesSettings = Field(default_factory=ProfilesSettings)
    application: ApplicationSettings = Field(default_factory=ApplicationSettings)
    ai: AiSettings = Field(default_factory=AiSettings)


# ------------------------- 其余顶层段 -------------------------
class ServerSettings(YmlModel):
    port: int = 9001


class LoggingSettings(YmlModel):
    level: dict[str, str] = Field(default_factory=dict)


# ------------------------- mcp.db 段 -------------------------
class PoolSettings(YmlModel):
    """连接池全局参数：映射到 SQLAlchemy QueuePool，每实例一个池。

    maximum-pool-size  → pool_size（max_overflow=0 固定池；实例可单独覆盖）
    connection-timeout → pool_timeout（借出等待超时；池满抛错让模型重试）
    max-lifetime       → pool_recycle（连接最大寿命，到寿自动重建）
    pool_pre_ping 恒开：借出前 ping，死连接当场换新（覆盖 keepalive/校验的作用）
    其余 Hikari 项（minimum-idle / idle-timeout / keepalive-time /
    validation-timeout / leak-detection-threshold / auto-commit）兼容保留。
    """

    maximum_pool_size: int = 5              # 池上限
    minimum_idle: int = 1                   # 兼容保留（懒建池，启动不预热）
    connection_timeout: int = 3000          # 借出等待超时（毫秒）
    validation_timeout: int = 2000          # 兼容保留
    idle_timeout: int = 60000               # 兼容保留（pre_ping + recycle 已覆盖）
    max_lifetime: int = 1740000             # 连接最大寿命
    keepalive_time: int = 300000            # 兼容保留（pre_ping 已覆盖）
    leak_detection_threshold: int = 0       # 兼容保留
    initialization_fail_timeout: int = 0    # 0=懒建池：首次借出才真正连接
    auto_commit: bool = True                # 兼容保留

    @model_validator(mode="after")
    def _sanity(self) -> "PoolSettings":
        if self.maximum_pool_size < 1:
            raise ValueError("mcp.db.pool：maximum-pool-size 须 ≥1")
        return self


class AsyncSettings(YmlModel):
    """异步查询：线程池 + 任务暂存区。"""

    core_pool_size: int = 10                # 兼容保留
    max_pool_size: int = 20                 # 工作线程数 = 同时在跑的查询上限
    queue_capacity: int = 64                # 排队上限，满了直接拒绝提交
    keep_alive_seconds: int = 60            # 兼容保留
    max_tasks: int = 200                    # 任务表上限，按最久未访问淘汰（RUNNING 不淘汰）
    ttl_minutes: int = 30                   # 任务保留时长


class ResultSettings(YmlModel):
    ttl_minutes: int = 40                   # 结果文件保留时长
    max_total_bytes: int = 10737418240      # 结果目录总容量兜底（10GB）
    download_base_url: str | None = None    # 必须由环境 profile 提供，缺失启动失败


class InstanceSettings(YmlModel):
    """一个数据源实例：一条 JDBC URL + 一套账号，挂多个库。"""

    url: str                                # jdbc:mysql://... / jdbc:sqlite:demo.db
    username: str = ""
    password: str = ""
    dialect: str | None = None              # 键名不是方言名时必须显式写
    catalogs: list[str] = Field(default_factory=list)
    driver_class_name: str = ""             # Java 专用，兼容保留
    maximum_pool_size: int | None = None    # 覆盖该实例的池上限（默认用全局值）
    data_source_properties: dict[str, str] = Field(default_factory=dict)


class DbSettings(YmlModel):
    pool: PoolSettings = Field(default_factory=PoolSettings)
    async_: AsyncSettings = Field(default_factory=AsyncSettings, alias="async")
    result: ResultSettings = Field(default_factory=ResultSettings)
    instances: dict[str, InstanceSettings] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _startup_checks(self) -> "DbSettings":
        if not self.instances:
            raise ValueError("mcp.db.instances 至少要配置一个数据源实例")
        # ① 结果 TTL 必须大于任务 TTL：否则任务还在、文件已被回收
        if self.result.ttl_minutes <= self.async_.ttl_minutes:
            raise ValueError(
                f"mcp.db.result.ttl-minutes({self.result.ttl_minutes}) 必须大于 "
                f"mcp.db.async.ttl-minutes({self.async_.ttl_minutes})，"
                "否则内存里任务还在、磁盘上结果已被删"
            )
        # ② download-base-url 必须有：返回一个下载不了的链接比启动失败更糟
        if not self.result.download_base_url:
            raise ValueError(
                "mcp.db.result.download-base-url 缺失："
                "请在 application-{profile}.yml 提供该环境实际可访问的地址"
            )
        # ③ 实例方言：键名是方言名可省略 dialect（用键名补全），否则必须显式写
        for name, ins in self.instances.items():
            if ins.dialect is None:
                if name not in KNOWN_DIALECTS:
                    raise ValueError(
                        f"实例 {name!r} 的键名不是方言名（{'/'.join(KNOWN_DIALECTS)}），"
                        "必须显式配置 dialect"
                    )
                ins.dialect = name
        return self


class McpSettings(YmlModel):
    db: DbSettings = Field(default_factory=DbSettings)


# ------------------------- 顶层 + YAML 加载 -------------------------
class Settings(BaseSettings):
    """整棵配置树。入口：Settings()——加载失败（缺环境变量/交叉校验不过）直接抛。"""

    model_config = SettingsConfigDict(extra="ignore")

    server: ServerSettings = Field(default_factory=ServerSettings)
    spring: SpringSettings = Field(default_factory=SpringSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    mcp: McpSettings = Field(default_factory=McpSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # 只留「显式传参 > application.yml(含 profile 覆盖)」两个来源；
        # 环境变量想进配置只能走 ${VAR} 占位符，避免 MYSQL_USER 这类变量被
        # 误当成整段字段覆盖
        return (init_settings, ProfileYamlSource(settings_cls))


def _expand_placeholders(text: str, source: Path) -> str:
    """展开 ${VAR} / ${VAR:默认值}。变量缺失且无默认值时报出变量名和出处。"""

    def _sub(m: re.Match[str]) -> str:
        name, default = m.group(1), m.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ValueError(
            f"{source.name} 引用的环境变量 {{{name}}} 未设置："
            f"请启动前设置它，或改用 {{{name}:默认值}} 写法"
        )

    return _PLACEHOLDER.sub(_sub, text)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Spring 语义的深合并：dict 按键合并，其余类型整体覆盖；返回新 dict 不改入参。"""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _active_profile(base: dict[str, Any]) -> str:
    for arg in sys.argv[1:]:
        if arg.startswith("--spring.profiles.active="):
            return arg.partition("=")[2]
    env = os.environ.get("SPRING_PROFILES_ACTIVE")
    if env:
        return env
    spring = base.get("spring") or {}
    return ((spring.get("profiles") or {}).get("active")) or "dev"


class ProfileYamlSource(PydanticBaseSettingsSource):
    """自定义配置源：主配置 + profile 覆盖 + 占位符展开，整树返回。"""

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        raise NotImplementedError  # 整树经 __call__ 返回，不走逐字段通道

    def __call__(self) -> dict[str, Any]:
        base = self._load(CONFIG_FILE)
        profile = _active_profile(base)
        overlay = self._load(CONFIG_FILE.with_name(f"application-{profile}.yml"))
        merged = _deep_merge(base, overlay)
        # 把实际生效的 profile 写回树里，运行期和日志都能读到
        return _deep_merge(merged, {"spring": {"profiles": {"active": profile}}})

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        text = _expand_placeholders(path.read_text(encoding="utf-8"), path)
        return yaml.safe_load(text) or {}
