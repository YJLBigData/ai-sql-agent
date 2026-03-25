from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .models import KnowledgeDocument
from .retriever import tokenize


@dataclass(frozen=True)
class MetricDefinition:
    code: str
    name: str
    aliases: tuple[str, ...]
    description: str
    formula: str
    source_tables: tuple[str, ...]
    related_dimensions: tuple[str, ...]
    security_level: str = "S1"


@dataclass(frozen=True)
class DimensionDefinition:
    code: str
    name: str
    aliases: tuple[str, ...]
    description: str
    source_tables: tuple[str, ...]
    field_name: str
    available: bool
    security_level: str = "S1"


@dataclass(frozen=True)
class SynonymDefinition:
    alias: str
    canonical: str
    synonym_type: str


@dataclass(frozen=True)
class TableRelationship:
    left_table: str
    right_table: str
    join_type: str
    join_keys: tuple[str, ...]
    description: str
    security_level: str = "S2"


@dataclass(frozen=True)
class FieldSemantic:
    table_name: str
    field_name: str
    business_name: str
    description: str
    security_level: str = "S2"


@dataclass(frozen=True)
class SQLExampleDefinition:
    code: str
    name: str
    path: str
    topic: str
    metrics: tuple[str, ...]
    dimensions: tuple[str, ...]
    tables: tuple[str, ...]
    security_level: str = "S1"


@dataclass(frozen=True)
class StructuredKnowledgeSummary:
    normalized_question: str
    matched_synonyms: tuple[dict[str, str], ...] = field(default_factory=tuple)


class StructuredKnowledgeBase:
    def __init__(self, knowledge_dir: Path) -> None:
        self.knowledge_dir = knowledge_dir
        self.structured_dir = knowledge_dir / "structured"
        self.metrics = self._load_metrics()
        self.dimensions = self._load_dimensions()
        self.synonyms = self._load_synonyms()
        self.table_relationships = self._load_relationships()
        self.field_semantics = self._load_field_semantics()
        self.sql_examples = self._load_sql_examples()
        self.metric_map = {item.code: item for item in self.metrics}
        self.dimension_map = {item.code: item for item in self.dimensions}
        self.synonym_map = {item.alias.lower(): item for item in self.synonyms}

    def normalize_question(self, question: str) -> StructuredKnowledgeSummary:
        normalized = question
        matched: list[dict[str, str]] = []
        for synonym in sorted(self.synonyms, key=lambda item: len(item.alias), reverse=True):
            pattern = re.compile(re.escape(synonym.alias), flags=re.IGNORECASE)
            if pattern.search(normalized):
                normalized = pattern.sub(synonym.canonical, normalized)
                matched.append(
                    {
                        "alias": synonym.alias,
                        "canonical": synonym.canonical,
                        "type": synonym.synonym_type,
                    }
                )
        return StructuredKnowledgeSummary(normalized_question=normalized, matched_synonyms=tuple(matched))

    def detect_metric_aliases(self, question: str) -> tuple[str, ...]:
        lowered = question.lower()
        hits = [
            metric.code
            for metric in self.metrics
            if any(alias.lower() in lowered for alias in metric.aliases)
        ]
        return tuple(dict.fromkeys(hits))

    def detect_dimension_aliases(self, question: str) -> tuple[str, ...]:
        lowered = question.lower()
        hits = [
            dimension.code
            for dimension in self.dimensions
            if dimension.available and any(alias.lower() in lowered for alias in dimension.aliases)
        ]
        return tuple(dict.fromkeys(hits))

    def build_cards(
        self,
        metrics: tuple[str, ...],
        dimensions: tuple[str, ...],
        tables: list[str],
        topic: str,
    ) -> list[str]:
        cards: list[str] = []
        for metric_code in metrics:
            metric = self.metric_map.get(metric_code)
            if metric:
                cards.append(
                    f"[结构化指标/{metric.name}]\n"
                    f"指标编码: {metric.code}\n"
                    f"口径: {metric.description}\n"
                    f"公式: {metric.formula}\n"
                    f"主表: {', '.join(metric.source_tables)}"
                )
        for dimension_code in dimensions:
            dimension = self.dimension_map.get(dimension_code)
            if dimension:
                availability = "可直接使用" if dimension.available else "当前演示库暂不可直接使用"
                cards.append(
                    f"[结构化维度/{dimension.name}]\n"
                    f"维度编码: {dimension.code}\n"
                    f"说明: {dimension.description}\n"
                    f"来源表: {', '.join(dimension.source_tables) if dimension.source_tables else '暂无'}\n"
                    f"状态: {availability}"
                )
        cards.extend(self._relationship_cards(tables))
        cards.extend(self._field_cards(tables))
        cards.extend(self._example_cards(metrics, dimensions, topic))
        return cards

    def build_online_context(
        self,
        metrics: tuple[str, ...],
        dimensions: tuple[str, ...],
        tables: list[str],
        topic: str,
    ) -> dict[str, object]:
        metric_cards = [self.metric_map[code].name for code in metrics if code in self.metric_map]
        dimension_cards = [self.dimension_map[code].name for code in dimensions if code in self.dimension_map]
        table_roles = [self._abstract_table_role(table_name) for table_name in tables]
        join_hints = [
            relationship.description
            for relationship in self.table_relationships
            if relationship.left_table in tables and relationship.right_table in tables
        ]
        field_meanings = [
            {
                "table": field.table_name,
                "field": field.field_name,
                "business_name": field.business_name,
                "description": field.description,
            }
            for field in self.field_semantics
            if field.table_name in tables
        ][:8]
        examples = [
            example.name
            for example in self.sql_examples
            if example.topic == topic and set(example.tables).intersection(tables)
        ][:4]
        return {
            "metrics": metric_cards,
            "dimensions": dimension_cards,
            "table_roles": table_roles,
            "join_hints": join_hints,
            "field_meanings": field_meanings,
            "example_hints": examples,
        }

    def as_documents(self) -> list[KnowledgeDocument]:
        documents: list[KnowledgeDocument] = []
        for metric in self.metrics:
            content = (
                f"指标名称: {metric.name}\n"
                f"指标编码: {metric.code}\n"
                f"别名: {', '.join(metric.aliases)}\n"
                f"指标口径: {metric.description}\n"
                f"公式: {metric.formula}\n"
                f"来源表: {', '.join(metric.source_tables)}"
            )
            documents.append(
                self._build_document(
                    doc_id=f"structured/metrics/{metric.code}.json",
                    title=metric.code,
                    category="structured_metrics",
                    content=content,
                    metadata={"kind": "metric", "metrics": [metric.code], "tables": list(metric.source_tables)},
                    security_level=metric.security_level,
                )
            )
        for dimension in self.dimensions:
            content = (
                f"维度名称: {dimension.name}\n"
                f"维度编码: {dimension.code}\n"
                f"别名: {', '.join(dimension.aliases)}\n"
                f"维度说明: {dimension.description}\n"
                f"来源表: {', '.join(dimension.source_tables) if dimension.source_tables else '暂无'}"
            )
            documents.append(
                self._build_document(
                    doc_id=f"structured/dimensions/{dimension.code}.json",
                    title=dimension.code,
                    category="structured_dimensions",
                    content=content,
                    metadata={"kind": "dimension", "dimensions": [dimension.code], "tables": list(dimension.source_tables)},
                    security_level=dimension.security_level,
                )
            )
        for synonym in self.synonyms:
            content = f"同义词: {synonym.alias}\n规范表达: {synonym.canonical}\n类型: {synonym.synonym_type}"
            documents.append(
                self._build_document(
                    doc_id=f"structured/synonyms/{synonym.alias}.json",
                    title=synonym.alias,
                    category="structured_synonyms",
                    content=content,
                    metadata={"kind": "synonym", "canonical": synonym.canonical},
                    security_level="S0",
                )
            )
        for relationship in self.table_relationships:
            title = f"{relationship.left_table}_to_{relationship.right_table}"
            content = (
                f"关联关系: {relationship.left_table} {relationship.join_type} {relationship.right_table}\n"
                f"关联键: {', '.join(relationship.join_keys)}\n"
                f"说明: {relationship.description}"
            )
            documents.append(
                self._build_document(
                    doc_id=f"structured/table_graph/{title}.json",
                    title=title,
                    category="structured_relationships",
                    content=content,
                    metadata={
                        "kind": "relationship",
                        "tables": [relationship.left_table, relationship.right_table],
                        "join_keys": list(relationship.join_keys),
                    },
                    security_level=relationship.security_level,
                )
            )
        for field in self.field_semantics:
            title = f"{field.table_name}.{field.field_name}"
            content = (
                f"字段: {field.table_name}.{field.field_name}\n"
                f"业务名: {field.business_name}\n"
                f"释义: {field.description}"
            )
            documents.append(
                self._build_document(
                    doc_id=f"structured/field_semantics/{title}.json",
                    title=title,
                    category="structured_fields",
                    content=content,
                    metadata={"kind": "field", "tables": [field.table_name], "fields": [field.field_name]},
                    security_level=field.security_level,
                )
            )
        for example in self.sql_examples:
            content = (
                f"样例名称: {example.name}\n"
                f"主题: {example.topic}\n"
                f"指标: {', '.join(example.metrics)}\n"
                f"维度: {', '.join(example.dimensions)}\n"
                f"主表: {', '.join(example.tables)}\n"
                f"文件: {example.path}"
            )
            documents.append(
                self._build_document(
                    doc_id=f"structured/sql_examples/{example.code}.json",
                    title=example.code,
                    category="structured_examples",
                    content=content,
                    metadata={
                        "kind": "sql_example",
                        "topic": example.topic,
                        "metrics": list(example.metrics),
                        "dimensions": list(example.dimensions),
                        "tables": list(example.tables),
                    },
                    security_level=example.security_level,
                )
            )
        return documents

    def _relationship_cards(self, tables: list[str]) -> list[str]:
        return [
            "[结构化关系]\n"
            f"{relationship.left_table} {relationship.join_type} {relationship.right_table} ON {', '.join(relationship.join_keys)}\n"
            f"{relationship.description}"
            for relationship in self.table_relationships
            if relationship.left_table in tables and relationship.right_table in tables
        ][:4]

    def _field_cards(self, tables: list[str]) -> list[str]:
        cards = [
            "[字段业务释义]\n"
            f"{field.table_name}.{field.field_name} ({field.business_name})\n"
            f"{field.description}"
            for field in self.field_semantics
            if field.table_name in tables
        ]
        return cards[:8]

    def _example_cards(self, metrics: tuple[str, ...], dimensions: tuple[str, ...], topic: str) -> list[str]:
        cards: list[str] = []
        for example in self.sql_examples:
            if example.topic != topic:
                continue
            if metrics and not set(metrics).intersection(example.metrics):
                continue
            if dimensions and not set(dimensions).intersection(example.dimensions):
                continue
            cards.append(
                "[高质量SQL样例]\n"
                f"样例: {example.name}\n"
                f"指标: {', '.join(example.metrics)}\n"
                f"维度: {', '.join(example.dimensions)}\n"
                f"表: {', '.join(example.tables)}"
            )
        return cards[:4]

    @staticmethod
    def _abstract_table_role(table_name: str) -> dict[str, str]:
        roles = {
            "fct_order_main": ("fact_orders", "订单主事实"),
            "fct_order_item": ("fact_order_items", "订单商品事实"),
            "fct_refund_main": ("fact_refunds", "退款主事实"),
            "fct_refund_item": ("fact_refund_items", "退款商品事实"),
            "dim_store": ("dim_store", "渠道门店维度"),
            "dim_user": ("dim_user", "用户维度"),
            "dim_product": ("dim_product", "商品维度"),
            "dim_warehouse": ("dim_warehouse", "仓库维度"),
            "fct_inventory_snapshot": ("fact_inventory_snapshot", "库存快照事实"),
            "fct_inventory_flow": ("fact_inventory_flow", "库存流水事实"),
        }
        alias, role = roles.get(table_name, (table_name, table_name))
        return {"alias": alias, "role": role}

    def _build_document(
        self,
        doc_id: str,
        title: str,
        category: str,
        content: str,
        metadata: dict[str, object],
        security_level: str,
    ) -> KnowledgeDocument:
        return KnowledgeDocument(
            doc_id=doc_id,
            title=title,
            category=category,
            path=self.knowledge_dir / doc_id,
            content=content,
            tokens=tuple(tokenize(content)),
            metadata=metadata,
            security_level=security_level,
        )

    def _load_metrics(self) -> tuple[MetricDefinition, ...]:
        return tuple(
            MetricDefinition(
                code=item["code"],
                name=item["name"],
                aliases=tuple(item["aliases"]),
                description=item["description"],
                formula=item["formula"],
                source_tables=tuple(item["source_tables"]),
                related_dimensions=tuple(item["related_dimensions"]),
                security_level=item.get("security_level", "S1"),
            )
            for item in self._load_json("metrics.json")
        )

    def _load_dimensions(self) -> tuple[DimensionDefinition, ...]:
        return tuple(
            DimensionDefinition(
                code=item["code"],
                name=item["name"],
                aliases=tuple(item["aliases"]),
                description=item["description"],
                source_tables=tuple(item["source_tables"]),
                field_name=item["field_name"],
                available=bool(item["available"]),
                security_level=item.get("security_level", "S1"),
            )
            for item in self._load_json("dimensions.json")
        )

    def _load_synonyms(self) -> tuple[SynonymDefinition, ...]:
        return tuple(
            SynonymDefinition(
                alias=item["alias"],
                canonical=item["canonical"],
                synonym_type=item["type"],
            )
            for item in self._load_json("synonyms.json")
        )

    def _load_relationships(self) -> tuple[TableRelationship, ...]:
        return tuple(
            TableRelationship(
                left_table=item["left_table"],
                right_table=item["right_table"],
                join_type=item["join_type"],
                join_keys=tuple(item["join_keys"]),
                description=item["description"],
                security_level=item.get("security_level", "S2"),
            )
            for item in self._load_json("table_graph.json")
        )

    def _load_field_semantics(self) -> tuple[FieldSemantic, ...]:
        return tuple(
            FieldSemantic(
                table_name=item["table_name"],
                field_name=item["field_name"],
                business_name=item["business_name"],
                description=item["description"],
                security_level=item.get("security_level", "S2"),
            )
            for item in self._load_json("field_semantics.json")
        )

    def _load_sql_examples(self) -> tuple[SQLExampleDefinition, ...]:
        return tuple(
            SQLExampleDefinition(
                code=item["code"],
                name=item["name"],
                path=item["path"],
                topic=item["topic"],
                metrics=tuple(item["metrics"]),
                dimensions=tuple(item["dimensions"]),
                tables=tuple(item["tables"]),
                security_level=item.get("security_level", "S1"),
            )
            for item in self._load_json("sql_examples.json")
        )

    def _load_json(self, file_name: str) -> list[dict[str, object]]:
        file_path = self.structured_dir / file_name
        return json.loads(file_path.read_text(encoding="utf-8"))
