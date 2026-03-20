from __future__ import annotations

import difflib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from time import perf_counter

from sql_ai_copilot.database.mysql_client import MySQLClient
from sql_ai_copilot.database.schema_service import SchemaService
from sql_ai_copilot.database.sql_validator import SQLValidator
from sql_ai_copilot.knowledge.models import KnowledgeDocument
from sql_ai_copilot.knowledge.retriever import LocalRetriever, tokenize
from sql_ai_copilot.llm.openai_compatible_client import OpenAICompatibleClient
from sql_ai_copilot.llm.prompt_builder import SYSTEM_PROMPT, build_repair_prompt, build_user_prompt
from sql_ai_copilot.logging_utils import get_logger
from sql_ai_copilot.semantic import DeterministicSQLPlanner, SemanticAnalyzer
from sql_ai_copilot.semantic.models import SemanticContext
from sql_ai_copilot.sql_meta import (
    is_query_task,
    normalize_sql_engine,
    normalize_task_mode,
    sql_engine_label,
    task_mode_label,
)


SCHEMA_DOC_TABLE_MAP = {
    "user": "dim_user",
    "order_main": "fct_order_main",
    "order_item": "fct_order_item",
    "refund_main": "fct_refund_main",
    "refund_item": "fct_refund_item",
    "product": "dim_product",
    "store": "dim_store",
    "warehouse": "dim_warehouse",
    "inventory_snapshot": "fct_inventory_snapshot",
    "inventory_flow": "fct_inventory_flow",
}

TABLE_KEYWORD_MAP = {
    "dim_user": ("dim_user", "用户", "user"),
    "dim_product": ("dim_product", "商品", "产品", "product"),
    "dim_store": ("dim_store", "门店", "store"),
    "dim_warehouse": ("dim_warehouse", "仓库", "warehouse"),
    "fct_order_main": ("fct_order_main", "订单主表", "订单表", "订单"),
    "fct_order_item": ("fct_order_item", "订单明细", "订单子表", "order_item"),
    "fct_refund_main": ("fct_refund_main", "退款主表", "退款表", "退款"),
    "fct_refund_item": ("fct_refund_item", "退款明细", "退款子表", "refund_item"),
    "fct_inventory_snapshot": ("fct_inventory_snapshot", "库存快照", "库存表", "库存"),
    "fct_inventory_flow": ("fct_inventory_flow", "库存流水", "出入库流水", "库存流水表"),
}

SEMANTIC_TABLE_HINTS = {
    "dim_user": ("用户", "会员", "注册", "复购", "拉新", "留存", "member", "user"),
    "dim_product": ("商品", "产品", "品牌", "类目", "系列", "spu", "sku", "product"),
    "dim_store": ("渠道", "门店", "大区", "省份", "城市", "区域", "store", "channel"),
    "dim_warehouse": ("仓库", "仓", "warehouse", "中心仓", "前置仓"),
    "fct_order_main": ("gmv", "销售额", "订单量", "订单数", "下单", "支付", "订单", "order"),
    "fct_order_item": ("商品明细", "订单明细", "销量", "件数", "sku销量", "top商品"),
    "fct_refund_main": ("退款", "退款率", "退款金额", "refund"),
    "fct_refund_item": ("退款明细", "退款商品", "商品退款", "退款top"),
    "fct_inventory_snapshot": ("库存", "库存量", "可用库存", "缺货", "快照"),
    "fct_inventory_flow": ("入库", "出库", "库存流水", "出入库", "调拨"),
}

SPECIAL_DOC_HINTS = {
    "gmv": ("gmv", "销售额", "交易额"),
    "order_count": ("订单量", "订单数", "单量"),
    "refund_rate": ("退款率", "退款金额", "退款"),
    "avg_order_amount": ("客单价",),
    "repeat_purchase_rate": ("复购", "复购率", "复购人数"),
    "fulfillment": ("履约", "履约率", "履约时长"),
    "demo_data_rules": ("蒙牛", "渠道", "退款率", "gmv"),
    "order_rules": ("订单", "gmv", "支付"),
    "refund_rules": ("退款", "退款率", "退款金额"),
    "ecommerce_semantics": ("gmv", "退款率", "系列", "类目", "渠道", "门店"),
    "inventory_rules": ("库存", "可用库存", "缺货", "入库", "出库", "仓库"),
    "product_refund_rules": ("系列", "类目", "商品", "退款", "退款率"),
    "inventory_qty": ("库存量", "可用库存", "缺货率", "库存"),
    "inventory_movement": ("入库", "出库", "库存流水", "出入库"),
    "gmv_by_channel": ("渠道", "gmv", "销售额", "交易额", "订单量"),
    "gmv_by_series": ("系列", "gmv", "销售额", "交易额"),
    "channel_gmv_refund_rate": ("渠道", "gmv", "退款金额", "退款率", "refund"),
    "repeat_purchase_by_channel": ("渠道", "复购", "复购率", "复购人数"),
    "fulfillment_by_channel": ("渠道", "履约", "履约率", "履约时长"),
    "gmv_mom_by_channel": ("渠道", "gmv", "环比", "mom"),
    "inventory_by_warehouse": ("仓库", "库存", "库存量", "可用库存", "缺货率"),
    "inventory_flow_by_series": ("系列", "入库", "出库", "库存流水"),
}

KNOWN_TABLE_NAMES = tuple(TABLE_KEYWORD_MAP)
TABLE_NAME_PATTERN = re.compile(r"\b(?:dim_user|dim_product|dim_store|dim_warehouse|fct_order_main|fct_order_item|fct_refund_main|fct_refund_item|fct_inventory_snapshot|fct_inventory_flow)\b")
TIME_GRAIN_HINTS = ("按天", "按日", "每日", "每天", "日期", "日趋势", "按周", "按月", "daily", "weekly", "monthly", "by day", "by week", "by month")
DIMENSION_GRAIN_HINTS = {
    "channel_name": ("渠道",),
    "store_name": ("门店",),
    "province_name": ("省份",),
    "city_name": ("城市",),
    "warehouse_name": ("仓库",),
    "warehouse_type": ("仓型", "仓库类型"),
    "series_name": ("系列",),
    "category_name": ("类目",),
    "product_name": ("商品", "sku"),
    "user_id": ("用户",),
}
MEASURE_GROUP_BY_HINTS = (
    "gmv",
    "refund_amount",
    "refund_rate",
    "order_cnt",
    "order_count",
    "paid_order_cnt",
    "refund_order_cnt",
    "payment_amount",
    "net_payment_amount",
    "discount_amount",
    "shipping_fee",
)


@dataclass
class SQLAgentResult:
    question: str
    sql: str
    documents: list[KnowledgeDocument]
    task_mode: str
    sql_engine: str
    rows: list[dict[str, object]] | None = None
    trace: dict[str, object] = field(default_factory=dict)
    message: str | None = None
    has_result_set: bool = False
    executed: bool = False


class SQLCopilotError(Exception):
    def __init__(
        self,
        message: str,
        trace: dict[str, object],
        sql: str,
        documents: list[KnowledgeDocument],
        task_mode: str,
        sql_engine: str,
        has_result_set: bool = False,
        executed: bool = False,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.trace = trace
        self.sql = sql
        self.documents = documents
        self.task_mode = task_mode
        self.sql_engine = sql_engine
        self.has_result_set = has_result_set
        self.executed = executed


class SQLClarificationRequired(Exception):
    def __init__(
        self,
        message: str,
        fields: list[str],
        choices: list[dict[str, str]],
        trace: dict[str, object],
        documents: list[KnowledgeDocument],
        task_mode: str,
        sql_engine: str,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.fields = fields
        self.choices = choices
        self.trace = trace
        self.documents = documents
        self.task_mode = task_mode
        self.sql_engine = sql_engine


class SQLCopilot:
    def __init__(
        self,
        client: MySQLClient,
        schema_service: SchemaService,
        retriever: LocalRetriever,
        validator: SQLValidator,
        llm_client: OpenAICompatibleClient,
        semantic_analyzer: SemanticAnalyzer,
        planner: DeterministicSQLPlanner,
    ) -> None:
        self.client = client
        self.schema_service = schema_service
        self.retriever = retriever
        self.validator = validator
        self.llm_client = llm_client
        self.semantic_analyzer = semantic_analyzer
        self.planner = planner
        self.logger = get_logger("agent")

    def generate_sql(
        self,
        question: str,
        provider_name: str,
        model_name: str,
        task_mode: str | None = None,
        sql_engine: str = "mysql",
    ) -> SQLAgentResult:
        return self._run_round(question, provider_name, model_name, execute=False, task_mode=task_mode, sql_engine=sql_engine)

    def run(
        self,
        question: str,
        provider_name: str,
        model_name: str,
        execute: bool = True,
        task_mode: str | None = None,
        sql_engine: str = "mysql",
    ) -> SQLAgentResult:
        return self._run_round(question, provider_name, model_name, execute=execute, task_mode=task_mode, sql_engine=sql_engine)

    def _run_round(
        self,
        question: str,
        provider_name: str,
        model_name: str,
        execute: bool,
        task_mode: str | None,
        sql_engine: str,
    ) -> SQLAgentResult:
        resolved_task_mode = self._resolve_task_mode(task_mode, question)
        resolved_sql_engine = self._resolve_sql_engine(resolved_task_mode, sql_engine)
        has_result_set = is_query_task(resolved_task_mode)
        semantic_context = self.semantic_analyzer.analyze(question, resolved_task_mode, resolved_sql_engine)
        documents = self.retriever.search(question, semantic_context=semantic_context)
        related_tables = self._resolve_relevant_tables(question, documents, semantic_context)
        documents = self._select_documents(question, documents, related_tables, semantic_context)
        schema_summary = self.schema_service.get_compact_schema_summary(semantic_context.relevant_columns, related_tables or None)
        knowledge_snippets = self._build_knowledge_snippets(question, documents, semantic_context)
        semantic_summary = self._build_semantic_summary(semantic_context)

        round_started_at = datetime.now().astimezone().isoformat(timespec="seconds")
        round_started_perf = perf_counter()
        trace: dict[str, object] = {
            "question": question,
            "provider": provider_name,
            "model": model_name,
            "execute": execute,
            "task_mode": resolved_task_mode,
            "sql_engine": resolved_sql_engine,
            "has_result_set": has_result_set,
            "related_tables": related_tables,
            "semantic": semantic_context.to_trace(),
            "started_at": round_started_at,
            "attempts": [],
            "documents": [
                {
                    "title": document.title,
                    "category": document.category,
                    "path": str(document.path),
                }
                for document in documents
            ],
        }

        if semantic_context.route == "blocked":
            trace["success"] = False
            trace["route"] = "blocked"
            trace["error"] = semantic_context.route_reason
            trace["ended_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
            trace["elapsed_ms"] = round((perf_counter() - round_started_perf) * 1000)
            self.logger.warning("agent_blocked %s", json.dumps(trace, ensure_ascii=False))
            raise SQLCopilotError(
                semantic_context.route_reason,
                trace,
                "",
                documents,
                resolved_task_mode,
                resolved_sql_engine,
                has_result_set=has_result_set,
                executed=False,
            )

        extra_guidance, clarification_fields, clarification_choices = self._build_partition_guidance(
            question,
            documents,
            resolved_task_mode,
        )
        if extra_guidance:
            trace["extra_guidance"] = list(extra_guidance)
        if clarification_fields:
            message = "检测到多个可用于分区的时间字段，请明确指定分区字段后再生成 SQL。"
            trace["clarification_fields"] = clarification_fields
            trace["clarification_choices"] = clarification_choices
            trace["success"] = False
            trace["ended_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
            trace["elapsed_ms"] = round((perf_counter() - round_started_perf) * 1000)
            self.logger.warning("agent_clarification_required %s", json.dumps(trace, ensure_ascii=False))
            raise SQLClarificationRequired(
                message,
                clarification_fields,
                clarification_choices,
                trace,
                documents,
                resolved_task_mode,
                resolved_sql_engine,
            )

        last_error = ""
        last_sql = ""
        max_attempts = 3

        planner_result = self.planner.plan(semantic_context, resolved_task_mode, resolved_sql_engine)
        if planner_result is not None:
            template_started_at = datetime.now().astimezone().isoformat(timespec="seconds")
            template_started_perf = perf_counter()
            template_sql = self._normalize_sql(planner_result.sql)
            attempt_trace = {
                "attempt_no": 1,
                "stage": "template",
                "provider": "local",
                "model": "rule_planner_v1",
                "base_url": "local://planner",
                "started_at": template_started_at,
                "ended_at": None,
                "elapsed_ms": 0,
                "request_payload": {
                    "route": planner_result.route,
                    "reason": planner_result.reason,
                    "semantic": semantic_context.to_trace(),
                    "related_tables": related_tables,
                    "schema_summary": schema_summary,
                    "knowledge_snippets": knowledge_snippets,
                },
                "response_id": None,
                "response_model": "rule_planner_v1",
                "response_text": template_sql,
                "normalized_sql": template_sql,
                "prompt_response_diff": "",
                "status": "success",
                "error": None,
            }
            last_sql = template_sql

            try:
                self.validator.validate(template_sql, resolved_task_mode, resolved_sql_engine)
                if has_result_set:
                    self._validate_query_semantics(question, template_sql)
                should_execute = execute and has_result_set and resolved_sql_engine == "mysql"
                rows = self.client.query(template_sql) if should_execute else None
                if should_execute and rows is not None:
                    self._validate_result_grain(question, rows)
                attempt_trace["ended_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                attempt_trace["elapsed_ms"] = round((perf_counter() - template_started_perf) * 1000)
                trace["attempts"].append(attempt_trace)
                trace["success"] = True
                trace["route"] = "template"
                trace["ended_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                trace["elapsed_ms"] = round((perf_counter() - round_started_perf) * 1000)
                trace["executed"] = should_execute
                if rows is not None:
                    trace["row_count"] = len(rows)
                if not has_result_set:
                    trace["execution_note"] = "非查询语句只做 SQL 生成与校验，不返回结果集。"
                elif not should_execute:
                    trace["execution_note"] = "当前为仅生成 SQL 模式，未执行查询。"
                message = self._build_success_message(
                    resolved_task_mode,
                    resolved_sql_engine,
                    has_result_set,
                    should_execute,
                )
                self.logger.info("agent_trace %s", json.dumps(trace, ensure_ascii=False))
                return SQLAgentResult(
                    question=question,
                    sql=template_sql,
                    documents=documents,
                    rows=rows,
                    trace=trace,
                    message=message,
                    task_mode=resolved_task_mode,
                    sql_engine=resolved_sql_engine,
                    has_result_set=has_result_set,
                    executed=should_execute,
                )
            except Exception as exc:
                last_error = str(exc)
                attempt_trace["ended_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                attempt_trace["elapsed_ms"] = round((perf_counter() - template_started_perf) * 1000)
                attempt_trace["status"] = "failed"
                attempt_trace["error"] = last_error
                trace["attempts"].append(attempt_trace)
                trace["route"] = "llm_fallback"
                self.logger.warning("agent_template_failed %s", json.dumps(attempt_trace, ensure_ascii=False))

        llm_attempt_offset = len(trace["attempts"])
        for llm_attempt_no in range(1, max_attempts + 1):
            attempt_no = llm_attempt_no + llm_attempt_offset
            stage = "generate" if llm_attempt_no == 1 else "repair"
            if llm_attempt_no == 1:
                prompt = build_user_prompt(
                    question,
                    schema_summary,
                    documents,
                    resolved_task_mode,
                    resolved_sql_engine,
                    extra_guidance,
                    semantic_summary=semantic_summary,
                    knowledge_snippets=knowledge_snippets,
                )
            else:
                prompt = build_repair_prompt(
                    question,
                    schema_summary,
                    documents,
                    last_sql,
                    last_error,
                    resolved_task_mode,
                    resolved_sql_engine,
                    extra_guidance,
                    semantic_summary=semantic_summary,
                    knowledge_snippets=knowledge_snippets,
                )

            response_text, llm_trace = self.llm_client.generate(provider_name, model_name, SYSTEM_PROMPT, prompt)
            sql = self._normalize_sql(response_text)
            attempt_trace = {
                "attempt_no": attempt_no,
                "stage": stage,
                **llm_trace,
                "normalized_sql": sql,
                "prompt_response_diff": self._build_prompt_response_diff(prompt, response_text),
                "status": "success",
                "error": None,
            }
            last_sql = sql

            try:
                self.validator.validate(sql, resolved_task_mode, resolved_sql_engine)
                if has_result_set:
                    self._validate_query_semantics(question, sql)
                should_execute = execute and has_result_set and resolved_sql_engine == "mysql"
                rows = self.client.query(sql) if should_execute else None
                if should_execute and rows is not None:
                    self._validate_result_grain(question, rows)
                trace["attempts"].append(attempt_trace)
                trace["success"] = True
                trace["route"] = "llm"
                trace["ended_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                trace["elapsed_ms"] = round((perf_counter() - round_started_perf) * 1000)
                trace["executed"] = should_execute
                if rows is not None:
                    trace["row_count"] = len(rows)
                if not has_result_set:
                    trace["execution_note"] = "非查询语句只做 SQL 生成与校验，不返回结果集。"
                elif not should_execute:
                    trace["execution_note"] = "当前为仅生成 SQL 模式，未执行查询。"

                message = self._build_success_message(
                    resolved_task_mode,
                    resolved_sql_engine,
                    has_result_set,
                    should_execute,
                )
                self.logger.info("agent_trace %s", json.dumps(trace, ensure_ascii=False))
                return SQLAgentResult(
                    question=question,
                    sql=sql,
                    documents=documents,
                    rows=rows,
                    trace=trace,
                    message=message,
                    task_mode=resolved_task_mode,
                    sql_engine=resolved_sql_engine,
                    has_result_set=has_result_set,
                    executed=should_execute,
                )
            except Exception as exc:
                last_error = str(exc)
                attempt_trace["status"] = "failed"
                attempt_trace["error"] = last_error
                trace["attempts"].append(attempt_trace)
                self.logger.warning("agent_attempt_failed %s", json.dumps(attempt_trace, ensure_ascii=False))

        trace["success"] = False
        trace["ended_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
        trace["elapsed_ms"] = round((perf_counter() - round_started_perf) * 1000)
        trace["error"] = last_error
        self.logger.error("agent_trace_failed %s", json.dumps(trace, ensure_ascii=False))
        raise SQLCopilotError(
            last_error,
            trace,
            last_sql,
            documents,
            resolved_task_mode,
            resolved_sql_engine,
            has_result_set=has_result_set,
            executed=False,
        )

    def _build_partition_guidance(
        self,
        question: str,
        documents: list[KnowledgeDocument],
        task_mode: str,
    ) -> tuple[list[str], list[str], list[dict[str, str]]]:
        lowered = question.lower()
        if "分区" not in question and "partition" not in lowered:
            return [], [], []
        if is_query_task(task_mode):
            return [], [], []

        relevant_tables = self._resolve_relevant_tables(question, documents)
        time_columns_map = self.schema_service.get_time_columns(relevant_tables or None)
        flat_fields: list[dict[str, str]] = []
        for table_name, columns in time_columns_map.items():
            for column in columns:
                comment = column["column_comment"] or column["column_name"]
                full_name = f"{table_name}.{column['column_name']}"
                flat_fields.append(
                    {
                        "label": f"{full_name}（{comment}）",
                        "full_name": full_name,
                        "column_name": column["column_name"],
                        "comment": comment,
                    }
                )

        if not flat_fields:
            return ["未发现可用时间字段，默认生成按日分区的拉链表，生命周期 30。"], [], []

        selected_field = self._match_explicit_partition_field(question, flat_fields)
        yesterday_value = (date.today() - timedelta(days=1)).isoformat()
        if selected_field:
            if "昨天" in question or yesterday_value in question:
                return [f"用户已指定分区字段 {selected_field['label']}，并要求使用昨天日期 {yesterday_value} 作为分区值，按日分区存储，生命周期永久。"], [], []
            return [f"用户已指定分区字段 {selected_field['label']}，按该字段做日分区存储，生命周期永久。"], [], []

        if "昨天" in question or yesterday_value in question:
            return [f"用户要求使用昨天日期 {yesterday_value} 作为分区值，默认创建日分区字段 dt，按日分区存储，生命周期永久。"], [], []

        if len(flat_fields) == 1:
            return [f"用户要求按分区表存储。默认使用 {flat_fields[0]['label']} 做按日分区存储，生命周期永久。"], [], []

        choices = [
            {"label": field["label"], "value": f"请使用分区字段 {field['full_name']}。"}
            for field in flat_fields
        ]
        choices.append({"label": f"昨天（{yesterday_value}）", "value": f"请使用昨天日期 {yesterday_value} 作为分区。"})
        return [], [field["label"] for field in flat_fields], choices

    def _resolve_relevant_tables(
        self,
        question: str,
        documents: list[KnowledgeDocument],
        semantic_context: SemanticContext | None = None,
    ) -> list[str]:
        lowered = question.lower()
        tables: list[str] = list(semantic_context.requested_tables) if semantic_context else []

        for table_name, keywords in TABLE_KEYWORD_MAP.items():
            if any(keyword.lower() in lowered for keyword in keywords):
                tables.append(table_name)

        for table_name, keywords in SEMANTIC_TABLE_HINTS.items():
            if any(keyword.lower() in lowered for keyword in keywords):
                tables.append(table_name)

        if ("gmv" in lowered or "退款率" in question or "refund rate" in lowered) and "fct_order_main" not in tables:
            tables.append("fct_order_main")
        if ("渠道" in question or "门店" in question or "store" in lowered or "channel" in lowered) and "dim_store" not in tables:
            tables.append("dim_store")
        if ("退款" in question or "refund" in lowered) and "fct_refund_main" not in tables:
            tables.append("fct_refund_main")
        if "库存" in question and "fct_inventory_snapshot" not in tables:
            tables.append("fct_inventory_snapshot")
        if any(keyword in question for keyword in ("入库", "出库", "库存流水")) and "fct_inventory_flow" not in tables:
            tables.append("fct_inventory_flow")
        if "仓库" in question and "dim_warehouse" not in tables:
            tables.append("dim_warehouse")

        if tables:
            return sorted(set(tables))

        for document in documents:
            table_name = SCHEMA_DOC_TABLE_MAP.get(document.title)
            if table_name:
                tables.append(table_name)

        return sorted(set(tables))

    def _select_documents(
        self,
        question: str,
        initial_documents: list[KnowledgeDocument],
        related_tables: list[str],
        semantic_context: SemanticContext | None = None,
    ) -> list[KnowledgeDocument]:
        lowered = question.lower()
        query_tokens = set(tokenize(question))
        related_table_set = set(related_tables)
        initial_ids = {document.doc_id for document in initial_documents}
        candidate_map = {document.doc_id: document for document in initial_documents}

        for document in self.retriever.documents:
            mapped_table = SCHEMA_DOC_TABLE_MAP.get(document.title)
            if mapped_table and mapped_table in related_table_set:
                candidate_map[document.doc_id] = document
                continue
            if any(table_name in document.content for table_name in related_table_set):
                candidate_map[document.doc_id] = document
                continue
            hint_keywords = SPECIAL_DOC_HINTS.get(document.title)
            if hint_keywords and any(keyword.lower() in lowered for keyword in hint_keywords):
                candidate_map[document.doc_id] = document

        def score(document: KnowledgeDocument) -> float:
            value = 0.0
            if document.doc_id in initial_ids:
                value += 10.0

            mapped_table = SCHEMA_DOC_TABLE_MAP.get(document.title)
            if mapped_table in related_table_set:
                value += 20.0

            doc_tables = set(TABLE_NAME_PATTERN.findall(document.content))
            if doc_tables & related_table_set:
                value += 8.0 * len(doc_tables & related_table_set)

            if document.category == "schema_docs" and mapped_table not in related_table_set:
                value -= 2.0

            matched_hint_count = 0
            if document.title in SPECIAL_DOC_HINTS:
                matched_hint_count = sum(1 for keyword in SPECIAL_DOC_HINTS[document.title] if keyword.lower() in lowered)
                value += matched_hint_count * 4.0
            if document.category == "sql_examples" and matched_hint_count:
                value += 12.0
            if document.category == "metrics" and matched_hint_count:
                value += 6.0

            value += len(query_tokens & set(document.tokens)) * 0.15
            if semantic_context:
                if document.title in semantic_context.metrics:
                    value += 8.0
                if document.title in {"gmv_by_channel", "channel_gmv_refund_rate"} and "channel_name" in semantic_context.dimensions:
                    value += 10.0
                if document.title == "gmv_by_series" and "series_name" in semantic_context.dimensions:
                    value += 10.0
                if any(metric in document.content.lower() for metric in semantic_context.metrics):
                    value += 2.5
            return value

        ranked = sorted(candidate_map.values(), key=score, reverse=True)
        selected: list[KnowledgeDocument] = []
        for table_name in related_tables:
            related_schema_doc = next(
                (
                    document
                    for document in ranked
                    if document.category == "schema_docs" and SCHEMA_DOC_TABLE_MAP.get(document.title) == table_name
                ),
                None,
            )
            if related_schema_doc and related_schema_doc not in selected:
                selected.append(related_schema_doc)

        for document in ranked:
            if score(document) <= 0 or document in selected:
                continue
            selected.append(document)
            if len(selected) >= 6:
                break

        selected = selected[:6]
        return selected or initial_documents

    def _validate_query_semantics(self, question: str, sql: str) -> None:
        lowered_question = question.lower()
        lowered_sql = sql.lower()
        outer_group_by = self._extract_last_group_by_clause(lowered_sql)

        if "渠道" in question and "channel_name" not in lowered_sql:
            raise ValueError("用户要求按渠道统计，SQL 必须使用 dim_store.channel_name 作为最终维度。")
        if "门店" in question and all(keyword not in lowered_sql for keyword in ("store_name", "store_id")):
            raise ValueError("用户要求按门店统计，SQL 必须使用门店维度字段作为最终维度。")
        if "系列" in question and "series_name" not in lowered_sql:
            raise ValueError("用户要求按系列统计，SQL 必须使用系列维度字段作为最终维度。")
        if ("类目" in question or "品类" in question) and "category_name" not in lowered_sql:
            raise ValueError("用户要求按类目统计，SQL 必须使用类目维度字段作为最终维度。")
        if ("商品" in question or "sku" in lowered_question) and "product_name" not in lowered_sql:
            raise ValueError("用户要求按商品统计，SQL 必须使用商品维度字段作为最终维度。")

        if outer_group_by:
            if any(token in outer_group_by for token in MEASURE_GROUP_BY_HINTS):
                raise ValueError("最外层 GROUP BY 不能包含 GMV、退款金额、订单量等度量字段，请只按最终分析维度聚合。")
            if "渠道" in question and not self._has_time_grain(question):
                if "store_id" in outer_group_by or "store_name" in outer_group_by:
                    raise ValueError("用户要求按渠道统计，最外层 GROUP BY 不应下钻到门店粒度。")
            if "门店" in question and not self._has_time_grain(question):
                if "channel_name" in outer_group_by:
                    raise ValueError("用户要求按门店统计，最外层 GROUP BY 不应只停留在渠道粒度。")

        if "退款率" in question and "refund_status = '退款成功'" not in lowered_sql and 'refund_status="退款成功"' not in lowered_sql:
            raise ValueError("退款率分析必须限定 refund_status = '退款成功'。")
        is_snapshot_inventory_question = any(keyword in question for keyword in ("可用库存", "预占库存", "缺货", "库存量", "库存金额")) or (
            "库存" in question and not any(keyword in question for keyword in ("入库", "出库", "流水"))
        )
        if is_snapshot_inventory_question:
            if not any(item in lowered_sql for item in ("fct_inventory_snapshot", "snapshot_agg", "inventory_qty", "available_qty", "reserved_qty")):
                raise ValueError("库存快照分析必须使用 fct_inventory_snapshot。")
        if any(keyword in question for keyword in ("入库", "出库", "库存流水")):
            if "fct_inventory_flow" not in lowered_sql and "flow_agg" not in lowered_sql:
                raise ValueError("库存流转分析必须使用 fct_inventory_flow。")
        if "仓库" in question and "warehouse_name" not in lowered_sql and "warehouse_type" not in lowered_sql:
            raise ValueError("用户要求按仓库或仓型统计，SQL 必须使用仓库维度字段。")
        if "缺货率" in question or "缺货" in question:
            if not any(item in lowered_sql for item in ("available_qty", "stock_status")):
                raise ValueError("缺货率分析必须使用 available_qty 或 stock_status。")
            if "count(distinct" not in lowered_sql:
                raise ValueError("缺货率分析必须基于缺货SKU数和SKU总数计算。")
        if any(keyword in lowered_question for keyword in ("gmv", "订单量", "订单数", "单量", "净销售额", "客单价", "复购", "履约")):
            if "pay_status = '已支付'" not in lowered_sql and 'pay_status="已支付"' not in lowered_sql:
                raise ValueError("订单类支付口径分析必须限定 pay_status = '已支付'。")
        if "客单价" in question:
            if not any(item in lowered_sql for item in ("payment_amount", "pay_amount")) or "count(distinct" not in lowered_sql:
                raise ValueError("客单价分析必须基于支付金额和去重订单数计算。")
        if "复购" in question:
            if "user_id" not in lowered_sql or (">= 2" not in lowered_sql and "order_count >= 2" not in lowered_sql):
                raise ValueError("复购分析必须基于用户维度识别时间窗内订单数>=2的用户。")
        if "履约" in question:
            if "finish_time" not in lowered_sql and "order_status" not in lowered_sql:
                raise ValueError("履约分析必须使用 finish_time 或 order_status。")
            if "履约时长" in question and "pay_time" not in lowered_sql:
                raise ValueError("履约时长分析必须使用 pay_time 和 finish_time。")
        if "环比" in question and not all(item in lowered_sql for item in ("current_", "previous_", "mom_rate")):
            raise ValueError("环比分析必须输出当前值、上期值和 mom_rate。")
        if "同比" in question and not all(item in lowered_sql for item in ("current_", "previous_", "yoy_rate")):
            raise ValueError("同比分析必须输出当前值、去年同期值和 yoy_rate。")
        if "gmv" in lowered_question and "payment_amount" not in lowered_sql:
            if not any(item in lowered_sql for item in ("pay_amount", "payment_amount")):
                raise ValueError("GMV 分析必须使用支付金额字段。")
        if any(keyword in question for keyword in ("系列", "类目", "商品")):
            if "gmv" in lowered_question and "pay_amount" not in lowered_sql:
                raise ValueError("商品维度 GMV 分析必须优先使用订单明细支付金额 pay_amount。")
            if ("退款金额" in question or "退款率" in question or "退款" in question) and "fct_refund_item" not in lowered_sql and "ri." not in lowered_sql:
                raise ValueError("商品维度退款分析必须优先使用退款明细事实。")

    def _validate_result_grain(self, question: str, rows: list[dict[str, object]]) -> None:
        if not rows or self._has_time_grain(question):
            return

        lowered_question = question.lower()
        requested_dimensions = [
            column_name
            for column_name, keywords in DIMENSION_GRAIN_HINTS.items()
            if any(keyword.lower() in lowered_question for keyword in keywords) and all(column_name in row for row in rows)
        ]
        if not requested_dimensions:
            return

        value_tuples = [
            tuple(row.get(column_name) for column_name in requested_dimensions)
            for row in rows
            if any(row.get(column_name) not in (None, "") for column_name in requested_dimensions)
        ]
        if value_tuples and len(value_tuples) != len(set(value_tuples)):
            raise ValueError(
                "查询结果中最终分析维度组合出现重复，结果粒度与用户问题不一致，请重新按最终维度聚合。"
            )

    def _build_semantic_summary(self, semantic_context: SemanticContext) -> str:
        lines = [
            f"- 路由策略: {semantic_context.route}（{semantic_context.route_reason}）",
            f"- 指标: {', '.join(semantic_context.metrics) if semantic_context.metrics else '未识别'}",
            f"- 维度: {', '.join(semantic_context.dimensions) if semantic_context.dimensions else '无显式维度'}",
            f"- 主题: {semantic_context.topic}",
            f"- 时间粒度: {semantic_context.time_grain or '无'}",
            f"- 时间范围: {semantic_context.time_window or '无'}",
            f"- 对比模式: {semantic_context.compare_mode or '无'}",
            f"- 主题族: {semantic_context.metric_family}",
            f"- 候选表: {', '.join(semantic_context.requested_tables) if semantic_context.requested_tables else '未识别'}",
        ]
        if semantic_context.hints:
            lines.append("- 语义提示: " + "；".join(semantic_context.hints))
        if semantic_context.notes:
            lines.append("- 业务说明: " + "；".join(semantic_context.notes))
        return "\n".join(lines)

    def _build_knowledge_snippets(
        self,
        question: str,
        documents: list[KnowledgeDocument],
        semantic_context: SemanticContext,
    ) -> list[str]:
        snippets: list[str] = []
        for document in documents:
            excerpt = self._compress_document_content(question, document, semantic_context)
            snippets.append(f"[{document.category}/{document.title}]\n{excerpt}")
        return snippets

    @staticmethod
    def _compress_document_content(
        question: str,
        document: KnowledgeDocument,
        semantic_context: SemanticContext,
    ) -> str:
        lines = [line.strip() for line in document.content.splitlines() if line.strip()]
        if len(lines) <= 8:
            return "\n".join(lines)

        lowered_question = question.lower()
        keywords = {
            *tokenize(question),
            *semantic_context.metrics,
            *semantic_context.dimensions,
            *semantic_context.requested_tables,
        }
        kept_lines: list[str] = []
        for line in lines:
            lowered_line = line.lower()
            if any(keyword and keyword.lower() in lowered_line for keyword in keywords):
                kept_lines.append(line)
                continue
            if any(word in lowered_line for word in ("粒度", "主键", "典型关联", "口径", "规则")):
                kept_lines.append(line)
                continue
            if "蒙牛" in lowered_question and "蒙牛" in line:
                kept_lines.append(line)
        if not kept_lines:
            kept_lines = lines[:8]
        return "\n".join(kept_lines[:10])

    @staticmethod
    def _extract_last_group_by_clause(sql: str) -> str:
        lowered = sql.lower()
        depth = 0
        in_single_quote = False
        in_double_quote = False
        group_start: int | None = None
        index = 0

        while index < len(lowered):
            char = lowered[index]
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
                index += 1
                continue
            if char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
                index += 1
                continue
            if in_single_quote or in_double_quote:
                index += 1
                continue

            if char == "(":
                depth += 1
                index += 1
                continue
            if char == ")":
                depth = max(depth - 1, 0)
                index += 1
                continue

            if depth == 0 and group_start is None and SQLCopilot._matches_keyword(lowered, index, "group by"):
                group_start = index + len("group by")
                index = group_start
                continue

            if depth == 0 and group_start is not None:
                if SQLCopilot._matches_keyword(lowered, index, "order by") or SQLCopilot._matches_keyword(lowered, index, "limit") or char == ";":
                    return lowered[group_start:index].strip()

            index += 1

        if group_start is None:
            return ""
        return lowered[group_start:].strip()

    @staticmethod
    def _matches_keyword(sql: str, index: int, keyword: str) -> bool:
        if not sql.startswith(keyword, index):
            return False
        if index > 0 and (sql[index - 1].isalnum() or sql[index - 1] == "_"):
            return False
        end_index = index + len(keyword)
        if end_index < len(sql) and (sql[end_index].isalnum() or sql[end_index] == "_"):
            return False
        return True

    @staticmethod
    def _has_time_grain(question: str) -> bool:
        lowered_question = question.lower()
        return any(keyword.lower() in lowered_question for keyword in TIME_GRAIN_HINTS)

    @staticmethod
    def _match_explicit_partition_field(question: str, fields: list[dict[str, str]]) -> dict[str, str] | None:
        lowered = question.lower()
        for field in fields:
            if field["full_name"].lower() in lowered:
                return field
            if field["column_name"].lower() in lowered:
                return field
            if field["comment"] and field["comment"] in question:
                return field
        return None

    @staticmethod
    def _build_success_message(task_mode: str, sql_engine: str, has_result_set: bool, executed: bool) -> str:
        if has_result_set:
            if executed:
                return "查询 SQL 已生成并执行完成。"
            return "查询 SQL 已生成并完成 MySQL 语法校验，当前为仅生成 SQL 模式。"
        return f"已生成 {task_mode_label(task_mode)} SQL，并完成 {sql_engine_label(sql_engine)} 语法规则校验。非查询语句不返回结果集。"

    @staticmethod
    def _resolve_task_mode(task_mode: str | None, question: str) -> str:
        normalized = normalize_task_mode(task_mode)
        if normalized:
            return normalized
        return SQLCopilot._infer_task_mode(question)

    @staticmethod
    def _resolve_sql_engine(task_mode: str, sql_engine: str) -> str:
        normalized = normalize_sql_engine(sql_engine)
        if is_query_task(task_mode):
            return "mysql"
        return normalized

    @staticmethod
    def _normalize_sql(text: str) -> str:
        normalized = text.strip()
        if normalized.startswith("```"):
            normalized = normalized.strip("`")
            normalized = normalized.replace("sql", "", 1).strip()
        return normalized.rstrip(";") + ";"

    @staticmethod
    def _build_prompt_response_diff(prompt: str, response_text: str) -> str:
        diff = difflib.unified_diff(
            prompt.splitlines(),
            response_text.splitlines(),
            fromfile="prompt",
            tofile="response",
            lineterm="",
        )
        return "\n".join(diff)

    @staticmethod
    def _infer_task_mode(question: str) -> str:
        lowered = question.lower()
        dcl_keywords = ("grant", "revoke", "授权", "赋权", "权限")
        ads_keywords = ("ads", "建表语句和insert", "建表并写入", "create table and insert", "落表并写入")
        ddl_keywords = ("建表", "创建表", "create table", "alter table", "drop table", "truncate", "分区表")
        dml_keywords = ("insert into", "insert", "update", "delete", "merge", "upsert", "写入", "插入", "更新", "删除", "修改")
        partition_keywords = ("分区", "partition", "lifecycle", "生命周期")
        output_keywords = ("落表", "写入", "入仓", "产出", "同步", "调度", "定时", "每天", "t+1", "t1")

        if any(keyword in lowered for keyword in dcl_keywords):
            return "dcl"
        if any(keyword in lowered for keyword in ads_keywords):
            return "ads_sql"
        if any(keyword in lowered for keyword in partition_keywords) and any(keyword in lowered for keyword in output_keywords):
            return "ads_sql"
        if any(keyword in lowered for keyword in ddl_keywords) and any(keyword in lowered for keyword in dml_keywords):
            return "ads_sql"
        if any(keyword in lowered for keyword in dml_keywords):
            return "dml"
        if any(keyword in lowered for keyword in ddl_keywords):
            return "ddl"
        return "dql"
