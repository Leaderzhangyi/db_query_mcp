# mcp-tutorial：只读多数据源查询 MCP（Python 版）

银行内部 Java 版 `db-mcp-server`（Spring Boot + Spring AI MCP）的教学复现。
两个工具：`query_db_async`（提交查询）→ `get_query_db_async_result`（查状态/拿结果），
结果只落盘给下载链接，绝不整包塞回模型上下文。

## 文件结构（配置 / 数据 / 入口分离）

| 文件 | 职责 |
|------|------|
| `config/application.yml` | 主配置，与 Java 版同构（profile / 占位符 / 结构见下） |
| `config/application-dev.yml` | dev 覆盖：stdio + 本地 SQLite 演示库 |
| `config/application-prod.yml` | prod 覆盖：真实 MySQL 实例（凭据走 `${MYSQL_USER}` 等） |
| `settings.py` | 配置读取（pydantic-settings）：profile 深合并 + `${ENV}` 展开 + 启动期交叉校验 |
| `database.py` | 数据访问：库名路由到实例、JDBC URL 解析、建连（sqlite / mysql） |
| `executor.py` | 数据处理：任务表（LRU/TTL）、工作线程池（满即拒绝）、CSV 流式落盘、结果回收 |
| `sql_guard.py` | SQL 守卫：sqlglot 解析成语法树，只放行单条只读 SELECT |
| `server.py` | 入口：装配 + MCP 工具注册 + 运行（stdio / http 按配置切换） |
| `smoke_test.py` | 冒烟测试（`python smoke_test.py`） |

## 运行

```bash
pip install -r requirements.txt        # 本地 sqlite 演示无需 pymysql

python server.py                       # dev（默认）：stdio，供 MCP 客户端拉起
SPRING_PROFILES_ACTIVE=prod python server.py
MYSQL_USER=... MYSQL_PASSWORD=... DOWNLOAD_BASE_URL=http://<对外地址>:<port>/results
                                       # prod：http，监听 server.port，凭据缺一启动即失败
python smoke_test.py                   # 冒烟测试
```

MCP 客户端 接入（stdio，工作区 `.mcp.json`）：

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

## 配置：与 Java 版 application.yml 兼容

同一棵配置树（`server` / `spring` / `logging` / `mcp.db.*`）Java、Python 两边都能读：

- **profile**：`--spring.profiles.active=prod` > 环境变量 `SPRING_PROFILES_ACTIVE`
  > 配置文件 `spring.profiles.active`（默认 dev）。覆盖文件 `application-{profile}.yml`
  深合并进主配置（Spring 语义：dict 按键合并、其余整体覆盖）。
- **占位符**：`${MYSQL_USER}` 缺失且无默认值 → 启动即报错（与 Spring 一致）；
  也支持 `${VAR:默认值}`。
- **交叉校验**（启动期，对应 Java 版）：
  `result.ttl-minutes` 必须大于 `async.ttl-minutes`；`download-base-url` 缺失不启动；
  实例键名不是方言名（mysql/oracle/postgresql/sqlite）必须显式写 `dialect`。

字段生效情况：

| 配置段 | 状态 |
|--------|------|
| `pool.maximum-pool-size / connection-timeout / max-lifetime` | 生效：映射 SQLAlchemy QueuePool（池上限 / 借出超时 / 最大寿命），借出前自动 ping 保活 |
| 实例的 `maximum-pool-size` | 生效：覆盖该实例的池上限 |
| `async.max-pool-size / queue-capacity / ttl-minutes / max-tasks` | 生效（线程池 + 拒绝 + 任务表） |
| `result.ttl-minutes / max-total-bytes / download-base-url` | 生效（文件回收 + 下载端点 `/results/{file}`） |
| `pool` 其余项、`async.core-pool-size / keep-alive-seconds`、实例的 `driver-class-name / data-source-properties` | 结构兼容保留 |
| `server.port`、`spring.ai.mcp.server.protocol` | 生效（streamable=HTTP 监听该端口；stdio 不占端口） |

## 注意

- 本机 9001 端口被 `SpaceAgent.exe`（终端安全软件）占用，演示 http 模式时
  需在 profile 里覆盖 `server.port`。
- mysql 方言用 SSCursor 服务端游标逐行拉取，与 sqlite 一致：内存与行数无关。
