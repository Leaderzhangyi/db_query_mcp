# db-query-mcp：只读多数据源查询 MCP 服务

基于 [FastMCP](https://github.com/PrefectHQ/fastmcp) 的只读数据库查询服务。
模型提交 SQL → 立即拿 queryId → 轮询拿结果文件；结果落成 CSV 供下载，
**绝不整包塞回模型上下文**。查询全程流式（服务端游标逐行拉取），内存与行数无关。

## 用到的 FastMCP 特性

| 特性 | 落点 |
|------|------|
| `instructions=` | 服务器级使用说明，客户端接入即见 |
| `Context` | 工具执行中给客户端发实时消息（提交/仍在执行） |
| `@mcp.resource` + URI 模板 | `db://databases` 浏览可查的库；`db://{database}/tables` 浏览表名——写 SQL 前先看资源，不用瞎猜表名 |
| `@mcp.prompt` | `query-assistant`：把自然语言问题转成安全 SELECT 的提示词模板 |
| `LoggingMiddleware` | 每次工具/资源调用自动一行操作日志（QUERY_LOG） |
| pydantic 返回类型 | 工具输出自动生成结构化 schema（`ToolReply`） |
| `mcp.custom_route` | http 模式的结果文件下载端点 `/results/{file}` |

## 文件结构（一个文件一件事）

| 文件 | 职责 |
|------|------|
| `config/config.yaml` | 主配置（公共部分） |
| `config/config.dev.yaml` | 开发环境覆盖层：stdio + SQLite 演示库 |
| `config/config.prod.yaml` | 生产环境覆盖层：MySQL 多主机 + http |
| `settings.py` | 配置读取（pydantic-settings：环境覆盖层深合并 + `${VAR}` 展开 + 启动校验） |
| `database.py` | 数据访问：库路由、SQLAlchemy 异步连接池、表清单 |
| `executor.py` | 数据处理：任务表（LRU/TTL）、asyncio 任务池、CSV 流式落盘、文件回收 |
| `sql_guard.py` | SQL 守卫：sqlglot 解析成语法树，只放行单条只读 SELECT |
| `server.py` | 入口：装配 FastMCP（工具/资源/提示词/中间件）并运行 |

## 运行

```bash
pip install -r requirements.txt          # 本地 SQLite 演示无需 aiomysql

python server.py                         # 默认 dev：stdio，供 MCP 客户端拉起
MCP_ENV=prod python server.py            # prod：http，监听 server.host:port
MYSQL_USER=... MYSQL_PASSWORD=... DOWNLOAD_BASE_URL=http://<对外地址>:<port>/results
python smoke_test.py                     # 冒烟测试（八段全绿）
python make_demo_db.py                   # 重建本地演示库 demo.db
```

MCP 客户端 接入（stdio，项目 `.mcp.json`）：

```json
{
  "mcpServers": {
    "db-query": {
      "command": "<本项目>/.venv/Scripts/python.exe",
      "args": ["<本项目>/server.py"]
    }
  }
}
```

http 模式接入地址：`http://<host>:<port>/mcp`。

## 配置

主配置 `config/config.yaml`，环境差异放 `config.{MCP_ENV}.yaml` 覆盖层
（`MCP_ENV` 默认 `dev`；覆盖层与主配置深合并：dict 按键合并、其余整体覆盖）。
值支持 `${ENV_VAR}` / `${ENV_VAR:默认值}` 占位符，变量缺失且无默认值时启动即报错。

启动期校验（不通过就拒绝启动）：

- `results.ttl_minutes` 必须大于 `query.task_ttl_minutes`（任务还在、文件不能先没）
- `transport: http` 时 `results.download_base_url` 必填（给一个下载不了的链接比报错更糟）
- 每个数据源必须写 `dialect`（sqlite / mysql）且配置齐全（path / hosts / catalogs）

连接池（每个数据源一个，SQLAlchemy 异步引擎）：`pool.size`（实例可用 `pool_size` 覆盖）、
`connect_timeout_ms`、`max_lifetime_s`、`pre_ping`（借出前 ping，死连接当场换新）。

## 注意

- 全链路 asyncio（stdio / http 都跑在事件循环上）：mysql 驱动 `aiomysql`、
  sqlite 驱动 `aiosqlite`，`SQLAlchemy[asyncio]` 依赖 greenlet。
- 查询经 `run_sync` + `stream_results` 服务端游标逐行拉取（原始 SQL 直达驱动，
  不走 `text()` 的 `:绑定参数` 解析）。
- 多台等价 mysql 主机（`hosts` 列表）在建引擎时随机选一台；要多机分担就配多个数据源。
- 本机 9001 端口可能被终端安全软件占用，http 演示时在覆盖层改 `server.port`。
