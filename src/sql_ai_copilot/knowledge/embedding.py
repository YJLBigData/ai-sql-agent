from __future__ import annotations

from pathlib import Path
import shutil

import numpy as np

from sql_ai_copilot.config.settings import EmbeddingSettings
from sql_ai_copilot.logging_utils import get_logger

try:
    from fastembed import TextEmbedding
except Exception:  # pragma: no cover - optional dependency fallback
    TextEmbedding = None


class LocalEmbeddingModel:
    def __init__(self, settings: EmbeddingSettings) -> None:
        self.settings = settings
        self.logger = get_logger("embedding")
        self._model: TextEmbedding | None = None
        self._disabled_reason: str | None = None
        self._recovered_once = False

    @property
    def enabled(self) -> bool:
        return self.settings.enabled and self._disabled_reason is None and TextEmbedding is not None

    @property
    def disabled_reason(self) -> str | None:
        return self._disabled_reason

    def embed_documents(self, texts: list[str]) -> np.ndarray | None:
        if not texts:
            return np.zeros((0, 0), dtype=np.float32)
        model = self._get_model()
        if model is None:
            return None
        vectors = np.asarray(list(model.passage_embed(texts)), dtype=np.float32)
        return self._normalize(vectors)

    def embed_query(self, text: str) -> np.ndarray | None:
        model = self._get_model()
        if model is None:
            return None
        vector = np.asarray(next(model.query_embed([text])), dtype=np.float32)
        normalized = self._normalize(vector.reshape(1, -1))
        return normalized[0]

    def _get_model(self) -> TextEmbedding | None:
        if not self.settings.enabled:
            self._disabled_reason = "本地 embedding 已关闭。"
            return None
        if TextEmbedding is None:
            self._disabled_reason = "fastembed 未安装，已回退到非向量检索。"
            self.logger.warning("embedding_disabled reason=%s", self._disabled_reason)
            return None
        if self._model is not None:
            return self._model
        self._hydrate_snapshot_from_extracted_model()
        try:
            self.logger.info(
                "embedding_model_load_start model=%s cache_dir=%s",
                self.settings.model_name,
                self.settings.cache_dir,
            )
            self._model = TextEmbedding(model_name=self.settings.model_name, cache_dir=str(self.settings.cache_dir))
            self.logger.info("embedding_model_load_done model=%s", self.settings.model_name)
            return self._model
        except Exception as exc:  # pragma: no cover - runtime/network fallback
            if not self._recovered_once and self._should_reset_cache(exc):
                self._recovered_once = True
                self.logger.warning("embedding_model_load_retry_reset_cache error=%s", exc)
                self._reset_cache_dir()
                return self._get_model()
            self._disabled_reason = f"本地 embedding 模型加载失败: {exc}"
            self.logger.warning("embedding_model_load_failed error=%s", exc)
            return None

    @staticmethod
    def _normalize(vectors: np.ndarray) -> np.ndarray:
        if vectors.size == 0:
            return vectors.astype(np.float32)
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        return (vectors / norms).astype(np.float32)

    @staticmethod
    def _should_reset_cache(exc: Exception) -> bool:
        message = str(exc)
        return "NO_SUCHFILE" in message or "File doesn't exist" in message or "failed:Load model" in message

    def _reset_cache_dir(self) -> None:
        try:
            if self.settings.cache_dir.exists():
                shutil.rmtree(self.settings.cache_dir)
            self.settings.cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # pragma: no cover - best effort cleanup
            self.logger.warning("embedding_cache_reset_failed error=%s", exc)

    def _hydrate_snapshot_from_extracted_model(self) -> None:
        model_tail = self.settings.model_name.split("/")[-1]
        extracted_dir = self.settings.cache_dir / f"fast-{model_tail}"
        hf_dir = self.settings.cache_dir / f"models--Qdrant--{model_tail}"
        refs_dir = hf_dir / "refs"
        ref_file = refs_dir / "main"
        ref_value = "manual-snapshot"
        if ref_file.exists():
            ref_value = ref_file.read_text(encoding="utf-8").strip() or ref_value
        snapshot_dir = hf_dir / "snapshots" / ref_value
        onnx_file = snapshot_dir / "model_optimized.onnx"
        if onnx_file.exists() or not extracted_dir.exists():
            return
        try:
            refs_dir.mkdir(parents=True, exist_ok=True)
            ref_file.write_text(ref_value, encoding="utf-8")
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            for item in extracted_dir.iterdir():
                if item.is_file():
                    shutil.copy2(item, snapshot_dir / item.name)
            self.logger.info("embedding_snapshot_hydrated snapshot_dir=%s", snapshot_dir)
        except Exception as exc:  # pragma: no cover - best effort hydration
            self.logger.warning("embedding_snapshot_hydrate_failed error=%s", exc)
