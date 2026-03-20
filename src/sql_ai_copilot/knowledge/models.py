from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class KnowledgeDocument:
    doc_id: str
    title: str
    category: str
    path: Path
    content: str
    tokens: tuple[str, ...] = field(default_factory=tuple)

