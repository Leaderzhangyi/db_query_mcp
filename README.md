# db_query_mcp

> 只读数据库查询 MCP 服务器：多环境数据源 · 异步查询 · 双层守卫 · **业务口径语义层**

[![MCP compatible](https://img.shields.io/badge/MCP-compatible-7c3aed.svg)](https://modelcontextprotocol.io)
[![FastMCP](https://img.shields.io/badge/FastMCP-4.x-7c3aed.svg)](https://gofastmcp.com)
[![Python](https://img.shields.io/badge/Python-3.10+-3776ab.svg?logo=python&logoColor=white)](https://www.python.org)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-d71f00.svg?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org)
[![SQL 守卫](https://img.shields.io/badge/SQL守卫-sqlglot-fb8b1e.svg)](https://github.com/tobymao/sqlglot)

[![语义层](https://img.shields.io/badge/语义层-Term·Mapping·Relation·Constraint-blue.svg)](config/ontology.yaml)
[![语义约束](https://img.shields.io/badge/语义约束-warn%20提示·block%20拦截-red.svg)](config/ontology.yaml)
[![轨迹](https://img.shields.io/badge/轨迹-EvoOntology%20兼容-blueviolet.svg)](trajectory.py)
[![冒烟测试](https://img.shields.io/badge/冒烟测试-⑫段全绿-brightgreen.svg)](smoke_test.py)
[![数据源](https://img.shields.io/badge/数据源-sqlite·mysql-47a3ff.svg)](config/config.yaml)

## TL;DR：这是个什么东西

给 AI Agent（Claude / Cursor / 任何 MCP 客户端）用的**只读**数据库查询服务。Agent 不直连数据库，通过本服务的工具查数：

| 能力 | 说明 |
|---|---|
| 多环境数据源 | 一个数据环境（stg1 / stg2 / dstg2 …）一条连接，按环境名或库名路由 |
| 异步查询 | 提交拿 queryId，轮询取结果；小结果直接内联回复，大结果落 CSV 给链接 |
| 双层守卫 | 语法层：只放行单条只读 SELECT；语义层：管口径对不对、敏感表碰不碰 |
| 语义层（ontology） | 业务概念 → 表字段 + 口径的对照表，模型写 SQL 前**先查表，不靠猜** |
| 轨迹落盘 | 每次查询自动记一条轨迹（EvoOntology 兼容格式），喂给语义层进化 |

---

## ❓ ontology（语义层）到底是干嘛的

### 没有 ontology：模型只能「看表名，猜语义」

模型收到问题：**「已支付金额是多少？」**

它能看到的是 `db://demo/tables` → `orders(id, customer, amount, status)`。

表名只告诉它**数据在哪**，没告诉它**「已支付」是什么意思**。于是它只能猜：

```sql
SELECT SUM(amount) FROM orders;          -- ❌ 猜错了：把 5 笔 PENDING 也算了进去
-- 结果 17212.5，其中含未支付 1822.5
```

demo 数据里错 1822.5，放在银行 3000 万行贷款表里，就是一笔永远对不上的账。

### 有 ontology：模型先查口径，再写 SQL

同样的请求，模型先调 `resolve_term("已支付金额")`，拿到**登记过的口径**：

```yaml
概念：已支付金额（别名：实付金额、支付总额）
  ├─ 落点：demo.orders.amount
  ├─ 怎么算：SUM(amount)
  ├─ 过滤：status = 'PAID'        ← 语义层告诉它的，不用猜
  └─ 粒度：一单一行
```

照口径写 SQL：

```sql
SELECT SUM(amount) FROM orders WHERE status = 'PAID';   -- ✅ 15390.0，对
```

> **一句话：ontology 就是把「业务黑话 → 表字段 + 口径」的对照表登记进
> `config/ontology.yaml`，做成 MCP 工具让模型写 SQL 前先查——
> 口径对了 SQL 才对。** 模型参考 [EvoOntology](https://github.com/ruc-datalab/EvoOntology)
> 的 Term / Mapping / Relation / Constraint 四类记录。

### 四类记录各管什么

| 记录 | 登记什么 | 回答模型什么问题 |
|---|---|---|
| **Term** | 业务概念 + 别名 + 定义 | 「实付金额」是登记的哪个概念？ |
| **Mapping** | 概念 → 库.表.字段 + 聚合/过滤/粒度 | 这个数在哪、怎么算才是对的？ |
| **Relation** | 概念之间的关联与连接条件 | 两个指标能不能一条 SQL 查出来？ |
| **Constraint** | 表/列上的口径雷区（warn/block） | 哪张表不能碰？哪列要小心？ |

### 一次查询的完整链路

```
用户问题：「已支付金额是多少？」
   │
   ▼
browse_terms("已支付金额")        ① 找概念（名字/别名/定义打分）
   │
   ▼
resolve_term("实付金额")          ② 拿口径：SUM(amount) WHERE status='PAID'
   │
   ▼
query_db_async(SQL)              ③ 语法守卫（单条只读 SELECT）
   │                                + 语义约束（block 拦下 / warn 附口径提示）
   ▼
get_query_db_async_result        ④ 小结果内联展示 / 大结果落 CSV
   │
   ▼
trajectories/<query_id>.json     ⑤ 自动落轨迹（EvoOntology 格式，供进化）
```

### 语义约束：warn 提醒，block 拦截

`config/ontology.yaml` 里的 `constraints` 在每次提交 SQL 时检查：

- **warn**（口径提醒）：查 `orders.amount` 的 SQL 会随回执附提示「仅 status='PAID' 为实付，先确认是否排除 PENDING」——放行，但模型当场看到雷。
- **block**（敏感拦截）：查 `customers` 表（含身份证号）直接拒绝，消息里给替代方案「客户维度分析请用 orders.customer 聚合」。

语法守卫（`sql_guard.py`）管「**安不安全**」，语义约束管「**口径对不对**」——两层各司其职。

---

## 工具与资源一览

| 类型 | 名称 | 用途 |
|---|---|---|
| 工具 | `query_db_async` | 异步提交只读 SQL（先过双层守卫），返回 queryId |
| 工具 | `get_query_db_async_result` | 轮询结果：小结果内联表 / 大结果 CSV 链接 |
| 工具 | `browse_terms` | 按自然语言找业务概念（打分排序） |
| 工具 | `resolve_term` | 概念名/别名/id → 完整口径 + 关联 + 约束 |
| 工具 | `export_trajectory` | 导出最近 N 条查询轨迹概览（EvoOntology 格式） |
| 资源 | `db://databases` | 数据环境与库清单 |
| 资源 | `db://{db}/tables` | 库的表清单 |
| 资源 | `db://terms` | 业务概念清单（语义层 manifest） |
| 提示词 | `query-assistant` | 两段式引导：先解析概念拿口径，再按口径写 SELECT |

## 快速开始

```bash
pip install -r requirements.txt
python make_demo_db.py        # 生成演示库：14 张表约 4700 行（零售银行全套）
python smoke_test.py          # 冒烟 ⑫ 段，全绿即就绪
python server.py              # dev 默认 http://localhost:9000/mcp
MCP_ENV=prod python server.py # 生产（银行 MySQL，凭据走环境变量占位符）
```

## 配置一览（config/config.yaml）

| 段 | 管什么 |
|---|---|
| `server` | 传输方式（stdio / http）、监听地址端口 |
| `databases` | 数据源实例：环境名 → 连接 + 库名清单（跨环境库名必须唯一） |
| `pool` | 连接池：上限、借出超时、连接寿命、pre-ping |
| `query` | 并发数、队列上限、任务 TTL、小结果内联阈值 |
| `results` | CSV 落盘目录、TTL、容量兜底、http 下载地址 |
| `ontology` | 语义层文件位置（缺失 = 语义层关闭，查询不受影响） |
| `trajectory` | 轨迹目录与内存保留条数 |
| `logging` | 双文件日志：app.log 全量 + error.log 只收 WARNING 以上 |

环境差异放 `config.{MCP_ENV}.yaml` 覆盖层（dev=sqlite 演示 / prod=银行 mysql），支持 `${VAR}` 占位符从环境变量注入凭据。

## 怎么维护语义层

### 不想从零写？结构部分自动起草

```bash
python draft_ontology.py            # 当前环境 → config/ontology.draft.yaml
python draft_ontology.py dstg2      # 指定环境/库
```

表、字段、注释自动从 schema 拉好（mysql 的表/列注释会带进 definition，
银行库的注释通常就是业务口径的原料），口径三件套留 TODO——人填空，
或连同 `trajectories/` 一起交给 LLM 起草。草稿**永不覆盖**正式文件，
审过再合并。

### 手工登记

往 `config/ontology.yaml` 加概念即可（yaml 有完整注释）：

```yaml
terms:
  - id: term_overdue_amount          # 概念唯一 id
    name: 逾期金额                    # 登记名
    aliases: [逾期本金, 逾期总额]      # 业务里的各种说法，检索靠它
    definition: 当前逾期未还的本金合计
    mappings:
      - database: dstg2              # 与 query_db_async 的 database 参数同口径
        table: loan_repay
        column: overdue_principal
        aggregation: SUM(overdue_principal)
        filter: "overdue_days > 0"
        grain: 一借据一行

constraints:
  - id: c_overdue_caliber
    table: loan_repay
    columns: [overdue_principal]
    severity: warn                   # warn=提醒放行 / block=直接拦截
    message: 逾期口径要剔除当日成立的贷款，参考概念「逾期金额」
```

配置错误启动即报错（不带病上岗）；文件缺失只是语义层关闭，查询照常。

## 轨迹与进化闭环

每次查询完成自动落一条 `trajectories/<query_id>.json`，字段对齐
[EvoOntology](https://github.com/ruc-datalab/EvoOntology) 的轨迹 schema
（`task_id / question / ontology_version / native_tool_calls / task_status / errors …`），
`split` 固定为 `rolling_trajectory`——没有固定测试集的生产 workload 也能进化。

闭环的完整循环（「自我」指的是这个循环里人只审最后一眼）：

```
ontology.yaml ──指导──> 查询 ──自动落──> trajectories/
     ▲                                        │
     │                          ① 诊断：哪类问题反复失败？哪个表反复触发 warn？
  ④ 人审 diff，合并                          │
     │                              ② LLM 起草增补（Claude / EvoOntology）
     └── ③ 提议的口径跑验证 SQL，通过才写回 <──┘
```

实际操作任选其一：

- **Claude 会话内进化**：跑一段时间后，把 `config/ontology.draft.yaml`
  （`draft_ontology.py` 生成）+ `trajectories/` 目录一起丢给 Claude——
  诊断轨迹、起草 Term/Constraint 增补、对每个提议给验证 SQL，你审完合并。
- **交给 EvoOntology**：`claude plugin marketplace add ruc-datalab/EvoOntology`，
  用它的 `/evoontology:evolve-ontology` 跑同样的循环（配对评估 + 门控发布
  ontology_vN+1），本项目的轨迹格式与其兼容。

## 项目结构

```
server.py        装配 + 入口：工具/资源/提示词注册，http 下载端点
settings.py      配置层：yaml + 环境覆盖层 + ${VAR} 占位符 + 启动校验
database.py      数据访问：库路由 + 异步连接池 + 表清单
executor.py      查询执行：任务表 / asyncio 任务池 / CSV 流式落盘 / 回收
sql_guard.py     语法守卫：单条只读 SELECT（sqlglot AST 级判断）
ontology.py      语义层：Term/Mapping/Relation/Constraint + 检索 + 约束检查
trajectory.py    轨迹：EvoOntology 格式落盘 + 概览导出
draft_ontology.py 语义层起草器：schema+注释 → 草稿 yaml（口径留 TODO）
config/          config.yaml（主）+ config.{env}.yaml（覆盖）+ ontology.yaml（口径）
smoke_test.py    冒烟测试 ⑫ 段：配置/守卫/全流程/池/下载/日志/语义/轨迹
```
