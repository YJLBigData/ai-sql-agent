from __future__ import annotations

from dataclasses import dataclass

from .models import KnowledgeDocument


@dataclass(frozen=True)
class RerankHit:
    document: KnowledgeDocument
    score: float
    reasons: tuple[str, ...]


class LocalReranker:
    def __init__(self) -> None:
        self.last_trace: dict[str, object] = {}

    def rerank(self, query: str, candidates: list[KnowledgeDocument], semantic_context, top_n: int = 6) -> list[KnowledgeDocument]:
        query_tokens = set(query.lower().split())
        hits: list[RerankHit] = []
        for index, document in enumerate(candidates):
            score = 0.0
            reasons: list[str] = []
            token_overlap = len(query_tokens.intersection(set(token.lower() for token in document.tokens)))
            score += token_overlap * 1.2
            if token_overlap:
                reasons.append(f"词项重合={token_overlap}")

            metadata = document.metadata or {}
            doc_tables = set(metadata.get("tables", []))
            doc_metrics = set(metadata.get("metrics", []))
            doc_dimensions = set(metadata.get("dimensions", []))

            if semantic_context is not None:
                if doc_tables.intersection(set(semantic_context.requested_tables)):
                    score += 8.0
                    reasons.append("命中候选表")
                if doc_metrics.intersection(set(semantic_context.metrics)):
                    score += 6.0
                    reasons.append("命中指标")
                if doc_dimensions.intersection(set(semantic_context.dimensions)):
                    score += 5.0
                    reasons.append("命中维度")
                if metadata.get("topic") == semantic_context.topic:
                    score += 4.0
                    reasons.append("命中主题")

            if document.category in {"structured_metrics", "structured_dimensions", "structured_relationships", "structured_fields"}:
                score += 2.6
                reasons.append("结构化知识加权")
            if document.category in {"sql_examples", "structured_examples"}:
                score += 1.8
                reasons.append("样例加权")
            if document.security_level == "S2":
                score += 0.4
            score += max(len(candidates) - index, 1) * 0.05
            hits.append(RerankHit(document=document, score=score, reasons=tuple(reasons)))

        hits.sort(key=lambda item: item.score, reverse=True)
        final_hits = hits[:top_n]
        self.last_trace = {
            "query": query,
            "candidates": len(candidates),
            "hits": [
                {
                    "title": hit.document.title,
                    "category": hit.document.category,
                    "score": round(hit.score, 3),
                    "reasons": list(hit.reasons),
                }
                for hit in final_hits
            ],
        }
        return [hit.document for hit in final_hits]
