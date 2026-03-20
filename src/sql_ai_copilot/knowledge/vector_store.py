from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from sql_ai_copilot.logging_utils import get_logger

from .embedding import LocalEmbeddingModel
from .models import KnowledgeDocument


class KnowledgeVectorStore:
    def __init__(self, embedding_model: LocalEmbeddingModel, index_dir: Path) -> None:
        self.embedding_model = embedding_model
        self.index_dir = index_dir
        self.logger = get_logger("vector_store")
        self.index_path = self.index_dir / "knowledge_vectors.npz"
        self.meta_path = self.index_dir / "knowledge_vectors.json"
        self.doc_ids: list[str] = []
        self.embeddings: np.ndarray | None = None

    @property
    def enabled(self) -> bool:
        return self.embedding_model.enabled and self.embeddings is not None and self.embeddings.size > 0

    def ensure_index(self, documents: list[KnowledgeDocument]) -> None:
        if not self.embedding_model.settings.enabled:
            self.logger.info("vector_store_disabled reason=embedding_setting_off")
            return
        signatures = self._build_signatures(documents)
        if self._load_from_disk(signatures):
            return
        vectors = self.embedding_model.embed_documents([document.content for document in documents])
        if vectors is None or vectors.size == 0:
            self.logger.warning("vector_store_build_skipped reason=%s", self.embedding_model.disabled_reason)
            return
        self.doc_ids = [document.doc_id for document in documents]
        self.embeddings = vectors
        self.index_dir.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(self.index_path, embeddings=vectors, doc_ids=np.asarray(self.doc_ids))
        self.meta_path.write_text(
            json.dumps(
                {
                    "model_name": self.embedding_model.settings.model_name,
                    "signatures": signatures,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.logger.info(
            "vector_store_build_done model=%s docs=%s dim=%s",
            self.embedding_model.settings.model_name,
            len(self.doc_ids),
            vectors.shape[1] if len(vectors.shape) > 1 else 0,
        )

    def search(self, query: str, top_k: int = 6) -> list[tuple[str, float]]:
        if not self.enabled:
            return []
        query_vector = self.embedding_model.embed_query(query)
        if query_vector is None or self.embeddings is None:
            return []
        scores = self.embeddings @ query_vector
        indices = np.argsort(-scores)[:top_k]
        results = [
            (self.doc_ids[index], float(scores[index]))
            for index in indices
            if scores[index] > 0
        ]
        self.logger.info("vector_store_search query=%s hits=%s", query, results)
        return results

    def _load_from_disk(self, signatures: dict[str, str]) -> bool:
        if not self.index_path.exists() or not self.meta_path.exists():
            return False
        try:
            meta = json.loads(self.meta_path.read_text(encoding="utf-8"))
            if meta.get("model_name") != self.embedding_model.settings.model_name:
                return False
            if meta.get("signatures") != signatures:
                return False
            loaded = np.load(self.index_path, allow_pickle=False)
            self.embeddings = loaded["embeddings"].astype(np.float32)
            self.doc_ids = loaded["doc_ids"].tolist()
            self.logger.info(
                "vector_store_load_done model=%s docs=%s",
                self.embedding_model.settings.model_name,
                len(self.doc_ids),
            )
            return True
        except Exception as exc:  # pragma: no cover - cache corruption fallback
            self.logger.warning("vector_store_load_failed error=%s", exc)
            return False

    @staticmethod
    def _build_signatures(documents: list[KnowledgeDocument]) -> dict[str, str]:
        return {
            document.doc_id: hashlib.sha256(document.content.encode("utf-8")).hexdigest()
            for document in documents
        }
