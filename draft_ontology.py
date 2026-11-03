"""从数据库 schema 自动起草语义层草稿（config/ontology.draft.yaml）。

为什么：手工从零写 ontology.yaml，最贵的不是 yaml 语法，而是「有哪些表、
哪些字段、注释写了什么」这些要翻库才知道的信息。本脚本把这部分机械工作
自动化——mysql 从 information_schema 拉表/列注释，sqlite 拉表结构
（sqlite 没有注释，只能给骨架）。口径三件套（聚合/过滤/粒度）机器猜不出
来，留 TODO：要么人填空，要么连同 trajectories/ 一起交给 LLM 起草，
验证后合并进 config/ontology.yaml。

用法：
  python draft_ontology.py                  # 默认环境的第一个库
  python draft_ontology.py dstg2            # 指定环境名或库名
  python draft_ontology.py --out 我的草稿.yaml

安全约定：只写草稿文件，永不覆盖 config/ontology.yaml——那边可能有
已确认的口径，合并必须走人审。
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path
from typing import Any

import yaml

import database
from settings import BASE_DIR, Settings


async def _fetch_schema(settings: Settings, db_name: str,
                        db: Any, catalog: str | None) -> tuple[dict[str, str], dict[str, list[dict]]]:
    """拉 schema：返回（表→表注释, 表→[{column,type,comment}]）。

    catalog 来自已校验的配置（与 database.list_tables 同一信任级别），
    可以安全内插进 information_schema 查询。
    """
    async with database.borrow(settings, db_name, db, catalog) as conn:
        if db.dialect == "sqlite":
            tables = [r[0] for r in (await conn.exec_driver_sql(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )).fetchall()]
            tcomments: dict[str, str] = {}
            schema: dict[str, list[dict]] = {}
            for table in tables:
                cols = (await conn.exec_driver_sql(f'PRAGMA table_info("{table}")')).fetchall()
                schema[table] = [{"column": c[1], "type": c[2] or "", "comment": ""}
                                 for c in cols]
            return tcomments, schema

        tcomments = dict((await conn.exec_driver_sql(
            "SELECT table_name, table_comment FROM information_schema.tables "
            f"WHERE table_schema = '{catalog}'")).fetchall())
        rows = (await conn.exec_driver_sql(
            "SELECT table_name, column_name, data_type, column_comment "
            f"FROM information_schema.columns WHERE table_schema = '{catalog}' "
            "ORDER BY table_name, ordinal_position")).fetchall()
        schema = {}
        for table, column, dtype, comment in rows:
            schema.setdefault(table, []).append(
                {"column": column, "type": dtype, "comment": comment or ""})
        return tcomments, schema


def _build_draft(db_label: str, tcomments: dict[str, str],
                 schema: dict[str, list[dict]]) -> dict[str, Any]:
    """schema → ontology 草稿 dict（结构填好、口径留 TODO）。

    每张表生成一个表级 Term：name 优先用表注释（mysql 业务库的注释往往
    就是业务概念名），definition 里把列清单+列注释带出来，给填口径的人
    （或 LLM）当上下文，不用再回去翻库。
    """
    terms = []
    for table, columns in schema.items():
        col_desc = "；".join(
            f"{c['column']}（{c['comment']}）" if c["comment"] else c["column"]
            for c in columns)
        terms.append({
            "id": f"term_{table.lower()}",
            "name": tcomments.get(table) or table,
            "aliases": [],                       # TODO：业务里的各种说法，检索靠它
            "definition": f"TODO：{table} 表级概念。字段：{col_desc}"[:800],
            "mappings": [{
                "database": db_label,
                "table": table,
                "column": "",                    # TODO：指标落在哪个字段（表级可留空）
                "aggregation": "",               # TODO：怎么算，如 SUM(x) / COUNT(*)
                "filter": "",                    # TODO：业务过滤，如 status='PAID'
                "grain": "",                     # TODO：明细一行是什么
            }],
        })
    return {
        "version": f"draft-{date.today().isoformat()}",
        "terms": terms,
        "relations": [],                         # TODO：跨表连接条件在这里补
        "constraints": [],                       # TODO：敏感表/口径雷区在这里补
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="从 schema 起草语义层草稿")
    parser.add_argument("database", nargs="?", default="",
                        help="环境名或库名（同 query_db_async 的 database 参数），留空用默认环境")
    parser.add_argument("--out", default="config/ontology.draft.yaml",
                        help="草稿输出路径（默认 config/ontology.draft.yaml）")
    args = parser.parse_args()

    settings = Settings()
    db_name, db, catalog = database.route(settings, args.database)
    tcomments, schema = await _fetch_schema(settings, db_name, db, catalog)
    draft = _build_draft(catalog or db_name, tcomments, schema)

    out = Path(args.out)
    if not out.is_absolute():
        out = BASE_DIR / out
    header = (
        "# 语义层草稿：draft_ontology.py 自动生成（结构来自 schema 与注释）。\n"
        "# 口径三件套（aggregation/filter/grain）与 aliases 是 TODO：人填空，\n"
        "# 或连同 trajectories/ 目录一起交给 LLM 起草；确认后【合并】进\n"
        "# config/ontology.yaml——不要直接覆盖，那边可能有已确认的口径。\n\n")
    out.write_text(header + yaml.safe_dump(draft, allow_unicode=True, sort_keys=False),
                   encoding="utf-8")

    n_cols = sum(len(cols) for cols in schema.values())
    n_comments = sum(1 for cols in schema.values() for c in cols if c["comment"])
    print(f"草稿就绪：{len(schema)} 张表 / {n_cols} 列（{n_comments} 列有注释）→ {out}")
    print("下一步：① 填 TODO（口径三件套 + aliases）；"
          "② 或把草稿与 trajectories/ 一起交给 LLM 起草；③ 审过后合并进 config/ontology.yaml")


if __name__ == "__main__":
    asyncio.run(main())
