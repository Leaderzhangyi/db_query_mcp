"""配置层：pydantic-settings 读取 config/config.yaml。

加载流程：
  ① 读 config.yaml（公共配置）
  ② 按 MCP_ENV 深合并 config.{env}.yaml（默认 dev；同名键覆盖、dict 按键合并）
  ③ 展开 ${ENV_VAR} / ${ENV_VAR:默认值} 占位符，变量缺失且无默认值 → 启动即报错
  ④ pydantic 校验，附带启动期交叉检查（见 Settings._startup_checks）
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

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config" / "config.yaml"

_PLACEHOLDER = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")

SUPPORTED_DIALECTS = ("sqlite", "mysql")


class ServerSettings(BaseModel):
    name: str = "db-query-server"
    transport: str = "stdio"            # stdio | http
    host: str = "0.0.0.0"
    port: int = 9001


class PoolSettings(BaseModel):
    """连接池默认值，每个数据库一个池。"""

    size: int = 5
    connect_timeout_ms: int = 3000
    max_lifetime_s: int = 1740
    pre_ping: bool = True


class QuerySettings(BaseModel):
    """查询执行：asyncio 任务池 + 任务暂存区。"""

    workers: int = 20                   # 同时执行的查询数
    queue_size: int = 64                # 排队上限，满了直接拒绝
    task_ttl_minutes: int = 30          # 任务记录保留时长
    max_tasks: int = 200                # 任务表上限，按最久未访问淘汰


class ResultsSettings(BaseModel):
    dir: str = "results"
    ttl_minutes: int = 40               # 结果文件保留时长
    max_total_bytes: int = 10737418240  # 目录总容量兜底（10GB）
    download_base_url: str = ""         # http 模式必填


class DatabaseSettings(BaseModel):
    """一个数据源：一套连接（mysql 可多台等价主机），下挂多个库。"""

    dialect: str                        # sqlite | mysql
    catalogs: list[str] = Field(default_factory=list)
    # sqlite：数据库文件路径（相对路径按项目目录解析）
    path: str = ""
    # mysql：host:port 列表（等价主机，新建连接随机挑一台）
    hosts: list[str] = Field(default_factory=list)
    username: str = ""
    password: str = ""
    pool_size: int | None = None        # 覆盖全局 pool.size


class Settings(BaseSettings):
    """整棵配置树。入口：Settings()——加载失败（缺环境变量/校验不过）直接抛。"""

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
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # 只留「显式传参 > config.yaml(含环境覆盖层)」；环境变量想进配置
        # 一律走 ${VAR} 占位符，避免误当整段字段覆盖
        return (init_settings, YamlSource(settings_cls))

    @model_validator(mode="after")
    def _startup_checks(self) -> "Settings":
        if not self.databases:
            raise ValueError("databases 至少要配置一个数据源")
        for name, db in self.databases.items():
            if db.dialect not in SUPPORTED_DIALECTS:
                raise ValueError(
                    f"databases.{name}.dialect={db.dialect!r} 不支持"
                    f"（可选：{'/'.join(SUPPORTED_DIALECTS)}）")
            if not db.catalogs:
                raise ValueError(f"databases.{name}.catalogs 不能为空")
            if db.dialect == "sqlite" and not db.path:
                raise ValueError(f"databases.{name} 是 sqlite，必须配置 path")
            if db.dialect == "mysql" and not db.hosts:
                raise ValueError(f"databases.{name} 是 mysql，必须配置 hosts")
            for host in db.hosts:
                _, _, port = host.partition(":")
                if not port.isdigit():
                    raise ValueError(f"databases.{name}.hosts 里 {host!r} 端口不是数字")
        # 结果文件要比任务记录活得久：否则任务还在、文件已被回收
        if self.results.ttl_minutes <= self.query.task_ttl_minutes:
            raise ValueError(
                f"results.ttl-minutes({self.results.ttl_minutes}) 必须大于 "
                f"query.task-ttl-minutes({self.query.task_ttl_minutes})，"
                "否则内存里任务还在、磁盘上结果已被删")
        # http 模式会返回下载链接，配不出一个能下载的地址就不该启动
        if self.server.transport == "http" and not self.results.download_base_url:
            raise ValueError("transport=http 时 results.download_base_url 必填"
                             "（在 config.{env}.yaml 里按部署环境提供）")
        return self


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
            f"请启动前设置它，或改用 {{{name}:默认值}} 写法")

    return _PLACEHOLDER.sub(_sub, text)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """覆盖层深合并：dict 按键合并，其余类型整体覆盖；返回新 dict 不改入参。"""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


class YamlSource(PydanticBaseSettingsSource):
    """自定义配置源：config.yaml + MCP_ENV 覆盖层 + 占位符展开，整树返回。"""

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        raise NotImplementedError  # 整树经 __call__ 返回，不走逐字段通道

    def __call__(self) -> dict[str, Any]:
        base = self._load(CONFIG_FILE)
        env = os.environ.get("MCP_ENV", "dev")
        overlay = self._load(CONFIG_FILE.with_name(f"config.{env}.yaml"))
        return _deep_merge(base, overlay)

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        text = _expand_placeholders(path.read_text(encoding="utf-8"), path)
        return yaml.safe_load(text) or {}
