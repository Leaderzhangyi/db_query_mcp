"""轨迹记录：把每次查询任务结构化落盘，供 EvoOntology 的 rolling_trajectory 模式进化消费。

为什么需要它：db_query_mcp 的真实 workload 是「业务问题 → 查口径 → 写 SQL →
执行 → 结果」的现成数据，而 EvoOntology（github.com/ruc-datalab/EvoOntology）
的进化正需要这类「没有固定测试集的轨迹」。本模块把每次查询对齐到它的
轨迹 schema，落成 trajectories/<task_id>.json，直接指给它的
/evoontology:evolve-ontology 即可用。

轨迹 schema 对齐 EvoOntology 的 trajectory.py（from_message_trace 产出）：
  task_id / question / ontology_version / split / semantic_calls /
  native_tool_calls / final_answer / task_status / errors / recorded_at

边界说明：MCP 工具服务器看不到客户端的对话，所以：
  - task 粒度 = 一次查询（query_id）；question / final_answer 由客户端
    在 export_trajectory 时补（缺省留空，不影响格式）
  - semantic_calls（browse/resolve 的关联）服务端无法可靠串到具体查询，
    留空，客户端可按需补
  - 大结果只截断保存（对齐 EvoOntology 的 truncate_result：2000 字符/20 行）
"""
from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from settings import BASE_DIR, Settings

# 截断阈值：对齐 EvoOntology 的 trajectory.py
_RESULT_MAX_CHARS = 2000
_RESULT_MAX_LINES = 20


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate(text: str) -> dict[str, Any]:
    """截断工具结果：小结果原样，大结果留预览 + 摘要（对齐 EvoOntology 约定）。"""
    lines = text.splitlines()
    if len(text) <= _RESULT_MAX_CHARS and len(lines) <= _RESULT_MAX_LINES:
        return {"result": text, "result_truncated": False, "result_summary": text}
    preview = "\n".join(lines[:_RESULT_MAX_LINES])[:_RESULT_MAX_CHARS]
    return {
        "result": preview,
        "result_truncated": True,
        "result_summary": f"{len(lines)} 行 / {len(text)} 字符，预览仅前 "
        f"{_RESULT_MAX_LINES} 行 / {_RESULT_MAX_CHARS} 字符",
    }


class TrajectoryRecorder:
    """查询轨迹记录器：每查询落一条 JSON，内存环形缓冲最近 N 条供导出。

    同步写盘（轨迹文件小、频率低，与 QUERY_LOG 同级开销）。只读 + 追加，
    不在事件循环里竞争共享状态。
    """

    def __init__(self, root: Path, ontology_version: str = "v0", max_records: int = 500):
        self.dir = root                # root 就是轨迹目录本身（调用方已解析好）
        self.dir.mkdir(parents=True, exist_ok=True)
        self._ontology_version = ontology_version
        self._summaries: deque[dict[str, Any]] = deque(maxlen=max_records)

    def record(self, *, task_id: str, sql: str, database: str, status: str,
               rows: int = 0, error: str = "", elapsed_ms: int = 0,
               question: str = "") -> dict[str, Any]:
        """落一条查询轨迹（SUCCESS / FAILED），返回写好的轨迹 dict。

        native_tool_calls 里放两条：query_db_async（提交，input=sql/database）
        和 get_query_db_async_result（取结果，result=状态摘要）。这是服务端
        能观察到的最小可信事实，不杜撰中间的语义推理。
        """
        ok = status == "SUCCESS"
        result_text = (f"SUCCESS：{rows} 行，耗时 {elapsed_ms}ms"
                       if ok else f"FAILED：{error}")
        trajectory = {
            "task_id": str(task_id),
            "question": str(question),
            "ontology_version": self._ontology_version,
            "split": "rolling_trajectory",
            "semantic_calls": [],   # 服务端无法可靠关联 browse/resolve 到具体查询
            "native_tool_calls": [
                {
                    "tool": "query_db_async",
                    "input": {"sql": sql, "database": database},
                },
                {
                    "tool": "get_query_db_async_result",
                    "input": {"query_id": str(task_id)},
                    **_truncate(result_text),
                },
            ],
            "final_answer": result_text,
            "task_status": "completed" if ok else "failed",
            "errors": [] if ok else [str(error)],
            "recorded_at": _now_iso(),
        }
        # task_id 是 uuid，天然安全，但写盘前仍校验文件名防穿越（防一手）
        path = self.dir / f"{_safe_component(str(task_id))}.json"
        path.write_text(json.dumps(trajectory, ensure_ascii=False, indent=2), encoding="utf-8")
        self._summaries.append({
            "task_id": str(task_id), "database": database, "status": status,
            "rows": rows, "sql": sql, "elapsed_ms": elapsed_ms,
        })
        return trajectory

    def summaries(self, limit: int = 20) -> list[dict[str, Any]]:
        """最近 limit 条概览（新在后）。返回副本，不暴露内部引用。"""
        return [dict(t) for t in list(self._summaries)[-limit:]]

    def count(self) -> int:
        return len(self._summaries)


def _safe_component(name: str) -> str:
    """轨迹文件名只允许 uuid 之类的安全字符，其余替换为下划线。"""
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in name) or "unknown"


def build_recorder(settings: Settings, ontology_version: str) -> TrajectoryRecorder:
    """按配置建记录器。轨迹目录相对路径按项目根解析，解析后即为轨迹目录本身。"""
    cfg = settings.trajectory
    root = Path(cfg.dir)
    if not root.is_absolute():
        root = BASE_DIR / root
    return TrajectoryRecorder(root, ontology_version=ontology_version, max_records=cfg.max_records)
