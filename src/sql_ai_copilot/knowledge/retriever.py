from __future__ import annotations

import math
import re
from collections import Counter
from typing import TYPE_CHECKING

from sql_ai_copilot.logging_utils import get_logger

from .models import KnowledgeDocument
from .reranker import LocalReranker

if TYPE_CHECKING:
    from sql_ai_copilot.semantic.models import SemanticContext
    from sql_ai_copilot.knowledge.vector_store import KnowledgeVectorStore


TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]")
TABLE_NAME_HINTS = (
    "dim_user",
    "fct_order_main",
    "fct_order_item",
    "fct_refund_main",
    "fct_refund_item",
    "dim_product",
    "dim_store",
    "dim_warehouse",
    "fct_inventory_snapshot",
    "fct_inventory_flow",
)


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def char_ngrams(text: str, min_n: int = 2, max_n: int = 3) -> list[str]:
    normalized = re.sub(r"\s+", "", text.lower())
    grams: list[str] = []
    for size in range(min_n, max_n + 1):
        if len(normalized) < size:
            continue
        grams.extend(normalized[index : index + size] for index in range(len(normalized) - size + 1))
    return grams


def cosine_similarity(left: Counter[str], right: Counter[str]) -> float:
    if not left or not right:
        return 0.0
    common = set(left) & set(right)
    numerator = sum(left[token] * right[token] for token in common)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


class LocalRetriever:
    def __init__(
        self,
        documents: list[KnowledgeDocument],
        vector_store: KnowledgeVectorStore | None = None,
        reranker: LocalReranker | None = None,
    ) -> None:
        self.documents = documents
        self.document_map = {document.doc_id: document for document in documents}
        self.doc_freq: Counter[str] = Counter()
        self.char_doc_freq: Counter[str] = Counter()
        self.logger = get_logger("retriever")
        self.vector_store = vector_store
        self.reranker = reranker
        self.last_trace: dict[str, object] = {}
        self.token_counters: dict[str, Counter[str]] = {}
        self.chargram_counters: dict[str, Counter[str]] = {}
        for document in documents:
            token_counter = Counter(document.tokens)
            char_counter = Counter(char_ngrams(document.content))
            self.token_counters[document.doc_id] = token_counter
            self.chargram_counters[document.doc_id] = char_counter
            self.doc_freq.update(set(document.tokens))
            self.char_doc_freq.update(set(char_counter))

    def search(self, query: str, top_k: int = 6, semantic_context: SemanticContext | None = None) -> list[KnowledgeDocument]:
        working_query = semantic_context.normalized_question if semantic_context and semantic_context.normalized_question else query
        query_terms = tokenize(working_query)
        query_counter = Counter(query_terms)
        query_char_counter = Counter(char_ngrams(working_query))
        vector_scores = self._vector_scores(working_query, semantic_context, top_k=max(top_k * 2, 8))
        if not query_terms:
            hits = self.documents[:top_k]
            self.logger.info("retriever_search_empty_query top_k=%s hits=%s", top_k, [document.title for document in hits])
            return hits

        total_docs = max(len(self.documents), 1)
        scored: list[tuple[float, KnowledgeDocument]] = []

        for document in self.documents:
            token_counter = self.token_counters[document.doc_id]
            char_counter = self.chargram_counters[document.doc_id]
            lexical_score = 0.0
            for token, qtf in query_counter.items():
                tf = token_counter.get(token, 0)
                if tf == 0:
                    continue
                idf = math.log((total_docs + 1) / (self.doc_freq[token] + 1)) + 1.0
                lexical_score += qtf * (tf / max(len(document.tokens), 1)) * idf * 100
            vector_score = self._vector_score(query_char_counter, char_counter, total_docs)
            score = lexical_score + vector_score + self._semantic_bonus(document, semantic_context)
            score += vector_scores.get(document.doc_id, 0.0) * 120
            if any(table_name in document.content for table_name in TABLE_NAME_HINTS):
                score += 1.2
            if score > 0:
                scored.append((score, document))

        scored.sort(key=lambda item: item[0], reverse=True)
        candidates = [item[1] for item in scored[: max(top_k * 3, 10)]] or self.documents[: max(top_k * 3, 10)]
        hits = self.reranker.rerank(working_query, candidates, semantic_context, top_n=top_k) if self.reranker else candidates[:top_k]
        self.last_trace = {
            "query": working_query,
            "top_k": top_k,
            "vector_enabled": bool(self.vector_store and self.vector_store.enabled),
            "candidate_titles": [f"{document.category}/{document.title}" for document in candidates[:top_k]],
            "hits": [f"{document.category}/{document.title}" for document in hits],
            "rerank": getattr(self.reranker, "last_trace", {}),
        }
        self.logger.info(
            "retriever_search query=%s top_k=%s semantic=%s hits=%s",
            working_query,
            top_k,
            semantic_context.to_trace() if semantic_context else None,
            [f"{document.category}/{document.title}" for document in hits],
        )
        return hits

    def _vector_scores(self, query: str, semantic_context: SemanticContext | None, top_k: int) -> dict[str, float]:
        if self.vector_store is None or not self.vector_store.enabled:
            return {}
        query_parts = [query]
        if semantic_context is not None:
            query_parts.extend(semantic_context.metrics)
            query_parts.extend(semantic_context.dimensions)
            query_parts.extend(semantic_context.requested_tables)
            query_parts.extend(semantic_context.hints[:2])
        vector_query = " ".join(part for part in query_parts if part)
        return dict(self.vector_store.search(vector_query, top_k=top_k))

    def _vector_score(self, query_char_counter: Counter[str], document_char_counter: Counter[str], total_docs: int) -> float:
        if not query_char_counter or not document_char_counter:
            return 0.0
        weighted_query = Counter()
        for gram, count in query_char_counter.items():
            idf = math.log((total_docs + 1) / (self.char_doc_freq[gram] + 1)) + 1.0
            weighted_query[gram] = count * idf
        weighted_doc = Counter()
        for gram, count in document_char_counter.items():
            idf = math.log((total_docs + 1) / (self.char_doc_freq[gram] + 1)) + 1.0
            weighted_doc[gram] = count * idf
        return cosine_similarity(weighted_query, weighted_doc) * 50

    @staticmethod
    def _semantic_bonus(document: KnowledgeDocument, semantic_context: SemanticContext | None) -> float:
        if semantic_context is None:
            return 0.0
        score = 0.0
        lowered_content = document.content.lower()
        for table_name in semantic_context.requested_tables:
            if table_name in lowered_content or table_name in document.title.lower():
                score += 9.0
        for metric in semantic_context.metrics:
            if metric.lower() in lowered_content or metric.lower() in document.title.lower():
                score += 4.0
        for dimension in semantic_context.dimensions:
            if dimension.lower() in lowered_content or dimension.lower() in document.title.lower():
                score += 3.0
        if semantic_context.metric_family == "product" and document.title in {"product", "gmv_by_series"}:
            score += 7.0
        if semantic_context.metric_family == "store" and document.title in {"store", "gmv_by_channel", "channel_gmv_refund_rate"}:
            score += 7.0
        if semantic_context.topic == "inventory" and document.title in {
            "warehouse",
            "inventory_snapshot",
            "inventory_flow",
            "inventory_rules",
            "inventory_qty",
            "inventory_movement",
            "inventory_by_warehouse",
            "inventory_flow_by_series",
        }:
            score += 8.0
        return score
