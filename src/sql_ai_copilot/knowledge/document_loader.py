from __future__ import annotations

from pathlib import Path

from .models import KnowledgeDocument
from .retriever import tokenize


class KnowledgeLoader:
    def __init__(self, knowledge_dir: Path) -> None:
        self.knowledge_dir = knowledge_dir

    def load(self) -> list[KnowledgeDocument]:
        documents: list[KnowledgeDocument] = []
        for file_path in sorted(self.knowledge_dir.rglob("*")):
            if not file_path.is_file() or file_path.suffix.lower() not in {".md", ".sql"}:
                continue
            category = file_path.parent.name
            content = file_path.read_text(encoding="utf-8").strip()
            if not content:
                continue
            documents.append(
                KnowledgeDocument(
                    doc_id=str(file_path.relative_to(self.knowledge_dir)),
                    title=file_path.stem,
                    category=category,
                    path=file_path,
                    content=content,
                    tokens=tuple(tokenize(content)),
                )
            )
        return documents

