"""语义层：业务概念（Term）→ 表/字段映射 + 指标口径 + 概念关联。

为什么需要它：表名和字段名只说明「数据在哪」，不说明「业务含义」。
「已支付金额」是 SUM(amount) 还是 SUM(amount) WHERE status='PAID'，
Agent 只能猜——猜错就是口径错误。语义层把这些口径显式登记进
config/ontology.yaml，通过两个 MCP 工具按需暴露（而不是整包塞 prompt）：

  browse_terms(query)   按自然语言找概念（名字/别名/定义匹配打分）
  resolve_term(term)    概念名/别名/id → 完整口径：哪库哪表哪字段、
                        怎么聚合、什么过滤、粒度是什么、有关联概念

数据模型参考 EvoOntology（github.com/ruc-datalab/EvoOntology）的
Term / Mapping / Relation 记录（其 Evidence / Constraint 与自进化机制
暂不引入；轨迹证据见 trajectory.py 的导出，语义约束见 check_sql）。

加载规则（与 settings.py 同一哲学）：文件缺失 = 空语义层，工具照常
注册、返回「未配置」，查询不受影响；文件存在但不合法 = 启动即报错，
不带病上岗。
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from settings import BASE_DIR, Settings

# 英文/数字词元切分（中文靠子串匹配，不切词）
_WORD = re.compile(r"[0-9A-Za-z_]+")


# ============================================================================
# yaml 记录模型：字段名 = ontology.yaml 的键名，pydantic 负责校验与默认值
# ============================================================================


class MappingSpec(BaseModel):
    """概念的一个落地点：哪个库哪张表哪个字段，以及口径三件套。"""

    database: str = ""        # 与 query_db_async 的 database 参数同口径（环境名或库名）
    table: str
    column: str = ""          # 空 = 整表级概念（如订单量 COUNT(*)）
    aggregation: str = ""     # 口径①：怎么算，如 SUM(amount) / COUNT(DISTINCT customer)
    filter: str = ""          # 口径②：业务过滤，如 status = 'PAID'
    grain: str = ""           # 口径③：粒度——明细一行是什么，如「一单一行」


class TermSpec(BaseModel):
    """业务概念：Agent 的说法 → 登记名。aliases 收得越全，检索越好用。"""

    id: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    definition: str = ""
    mappings: list[MappingSpec] = Field(default_factory=list)


class RelationSpec(BaseModel):
    """概念间关联：同表可合并查询，跨表给连接条件。source/target 都是 term id。"""

    source: str
    target: str
    type: str = ""            # 关系类型（same_table / join ...）
    condition: str = ""       # 连接条件或合并说明


class OntologyDoc(BaseModel):
    """ontology.yaml 整文件。version 会写进导出的轨迹（对应 EvoOntology 的版本号）。"""

    version: str = "v0"
    terms: list[TermSpec] = Field(default_factory=list)
    relations: list[RelationSpec] = Field(default_factory=list)


# ============================================================================
# 工具返回模型（server.py 两个工具的结构化输出）
# ============================================================================


class TermCard(BaseModel):
    """browse_terms 结果的一行：够 Agent 决定要不要进一步 resolve。"""

    id: str
    name: str
    definition: str = ""


class BrowseReply(BaseModel):
    success: bool
    message: str = ""
    terms: list[TermCard] = Field(default_factory=list)


class MappingCard(BaseModel):
    """resolve_term 返回的落地点（字段与 MappingSpec 一致）。"""

    database: str = ""
    table: str
    column: str = ""
    aggregation: str = ""
    filter: str = ""
    grain: str = ""


class RelationCard(BaseModel):
    """关联概念。other 用登记名（不是 id），Agent 直接能读。"""

    other: str
    type: str = ""
    condition: str = ""


class ResolveReply(BaseModel):
    success: bool
    message: str = ""
    id: str = ""
    name: str = ""
    definition: str = ""
    aliases: list[str] = Field(default_factory=list)
    mappings: list[MappingCard] = Field(default_factory=list)
    relations: list[RelationCard] = Field(default_factory=list)


# ============================================================================
# 内存索引：只读，只在事件循环里被访问，不需要锁
# ============================================================================


class OntologyStore:
    """语义层索引：browse 打分检索、resolve 精确解析。"""

    def __init__(self, doc: OntologyDoc | None = None):
        doc = doc or OntologyDoc()
        self.version = doc.version
        self._terms: list[TermSpec] = list(doc.terms)
        self._name_of: dict[str, str] = {t.id: t.name for t in doc.terms}
        # 名字/别名/id（一律小写）→ Term；名字优先，冲突时先登记者胜
        self._lookup: dict[str, TermSpec] = {}
        for term in doc.terms:
            self._lookup[term.name.lower()] = term
            for alias in term.aliases:
                self._lookup.setdefault(alias.lower(), term)
            self._lookup.setdefault(term.id.lower(), term)
        self._relations: list[RelationSpec] = list(doc.relations)

    def __bool__(self) -> bool:
        """空语义层（没配 yaml）按 False 处理，工具层据此返回「未配置」。"""
        return bool(self._terms)

    def all_terms(self) -> list[TermSpec]:
        return list(self._terms)

    @property
    def term_names(self) -> list[str]:
        return [t.name for t in self._terms]

    def browse(self, query: str, limit: int = 5) -> list[TermSpec]:
        """按自然语言找概念，按得分降序取前 limit 个。

        打分：名字子串命中 10 分 > 别名子串命中 8 分 > 定义词元重叠 1 分/词。
        中文不切词，靠子串；英文（如 NPL、dstg2）靠词元交集。
        """
        q = (query or "").strip()
        if not q or not self._terms:
            return []
        q_tokens = set(_WORD.findall(q.lower()))
        scored: list[tuple[int, TermSpec]] = []
        for term in self._terms:
            score = 0
            if term.name and term.name in q:
                score += 10
            score += 8 * sum(1 for a in term.aliases if a and a in q)
            body = f"{term.name} {term.definition}"
            score += len(q_tokens & set(_WORD.findall(body.lower())))
            if score > 0:
                scored.append((score, term))
        scored.sort(key=lambda pair: (-pair[0], pair[1].name))
        return [term for _, term in scored[:limit]]

    def resolve(self, term: str) -> TermSpec | None:
        """概念名/别名/id 精确解析（忽略大小写、去首尾空白）。找不到返回 None。"""
        return self._lookup.get((term or "").strip().lower())

    def relations_of(self, term: TermSpec) -> list[RelationCard]:
        """该概念的所有关联（作为 source 或 target 都算，双向列出）。"""
        cards: list[RelationCard] = []
        for rel in self._relations:
            if rel.source == term.id:
                other = self._name_of.get(rel.target, rel.target)
                cards.append(RelationCard(other=other, type=rel.type, condition=rel.condition))
            elif rel.target == term.id:
                other = self._name_of.get(rel.source, rel.source)
                cards.append(RelationCard(other=other, type=rel.type, condition=rel.condition))
        return cards


# ============================================================================
# 加载入口
# ============================================================================


def load_ontology(settings: Settings) -> OntologyStore:
    """按 settings.ontology.file 读语义层。

    文件缺失 → 空语义层（查询照常，只是没有概念可查）；
    文件存在但不合法（yaml 语法错 / 引用了未登记的概念 id）→ 抛 ValueError，
    让启动当场失败——语义口径错了比没有语义更糟。
    """
    path = Path(settings.ontology.file)
    if not path.is_absolute():
        path = BASE_DIR / path
    if not path.exists():
        return OntologyStore()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    doc = OntologyDoc.model_validate(raw)
    known_ids = {t.id for t in doc.terms}
    for rel in doc.relations:
        for endpoint in (rel.source, rel.target):
            if endpoint not in known_ids:
                raise ValueError(
                    f"{path.name}：relation 引用了未登记的概念 id {endpoint!r}"
                    f"（可选：{'、'.join(sorted(known_ids)) or '（无）'}）")
    return OntologyStore(doc)
