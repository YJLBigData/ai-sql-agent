from __future__ import annotations

from dataclasses import dataclass, field

from sql_ai_copilot.knowledge.models import KnowledgeDocument
from sql_ai_copilot.knowledge.structured_knowledge import StructuredKnowledgeBase


@dataclass(frozen=True)
class SecurityDecision:
    level: str
    reason: str
    allow_online: bool
    masked_context: dict[str, object] = field(default_factory=dict)

    def to_trace(self) -> dict[str, object]:
        return {
            "level": self.level,
            "reason": self.reason,
            "allow_online": self.allow_online,
            "masked_context": self.masked_context,
        }


class SecurityRouter:
    def __init__(self, knowledge_base: StructuredKnowledgeBase) -> None:
        self.knowledge_base = knowledge_base

    def classify(self, question: str, semantic_context, documents: list[KnowledgeDocument], related_tables: list[str]) -> SecurityDecision:
        if any(document.security_level == "S2" for document in documents):
            level = "S2"
            reason = "命中私有 schema、字段释义或高敏业务规则，只能本地处理。"
        elif any(document.security_level == "S1" for document in documents) or related_tables:
            level = "S1"
            reason = "命中内部指标、维度或样例，可脱敏后发送给在线模型。"
        else:
            level = "S0"
            reason = "仅命中公开语义词典，可直接发送给在线模型。"

        if semantic_context.topic in {"repeat_purchase", "inventory"}:
            level = "S2"
            reason = "用户画像或库存明细属于敏感主题，固定本地处理。"
        if any(table_name in {"dim_user", "fct_inventory_snapshot", "fct_inventory_flow"} for table_name in related_tables):
            level = "S2"
            reason = "涉及用户或库存事实表，固定本地处理。"

        allow_online = level != "S2"
        masked_context = self.knowledge_base.build_online_context(
            semantic_context.metrics,
            semantic_context.dimensions,
            related_tables,
            semantic_context.topic,
        )
        return SecurityDecision(level=level, reason=reason, allow_online=allow_online, masked_context=masked_context)
