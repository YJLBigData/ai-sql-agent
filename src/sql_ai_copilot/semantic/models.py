from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SemanticContext:
    question: str
    normalized_question: str = ""
    metrics: tuple[str, ...] = ()
    dimensions: tuple[str, ...] = ()
    time_grain: str | None = None
    time_window: str | None = None
    time_window_value: int | None = None
    compare_mode: str | None = None
    sort_metric: str | None = None
    sort_desc: bool = True
    limit: int | None = None
    metric_family: str = "store"
    topic: str = "sales"
    requested_tables: tuple[str, ...] = ()
    relevant_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    hints: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    route: str = "llm"
    route_reason: str = "未命中本地模板。"
    matched_synonyms: tuple[dict[str, str], ...] = ()

    def to_trace(self) -> dict[str, object]:
        return {
            "normalized_question": self.normalized_question or self.question,
            "metrics": list(self.metrics),
            "dimensions": list(self.dimensions),
            "time_grain": self.time_grain,
            "time_window": self.time_window,
            "time_window_value": self.time_window_value,
            "compare_mode": self.compare_mode,
            "sort_metric": self.sort_metric,
            "sort_desc": self.sort_desc,
            "limit": self.limit,
            "metric_family": self.metric_family,
            "topic": self.topic,
            "requested_tables": list(self.requested_tables),
            "relevant_columns": {key: list(value) for key, value in self.relevant_columns.items()},
            "hints": list(self.hints),
            "notes": list(self.notes),
            "route": self.route,
            "route_reason": self.route_reason,
            "matched_synonyms": list(self.matched_synonyms),
        }
