from __future__ import annotations

import sys
from pathlib import Path
import json
from functools import lru_cache

from flask import Flask, jsonify, render_template, request

if __package__ in {None, ""}:
    src_dir = Path(__file__).resolve().parents[1]
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

from sql_ai_copilot.agent.sql_copilot import SQLClarificationRequired, SQLCopilot
from sql_ai_copilot.agent.sql_copilot import SQLCopilotError
from sql_ai_copilot.config.settings import get_settings
from sql_ai_copilot.database.mysql_client import MySQLClient
from sql_ai_copilot.database.schema_service import SchemaService
from sql_ai_copilot.database.sql_validator import SQLValidator
from sql_ai_copilot.knowledge.document_loader import KnowledgeLoader
from sql_ai_copilot.knowledge.embedding import LocalEmbeddingModel
from sql_ai_copilot.knowledge.reranker import LocalReranker
from sql_ai_copilot.knowledge.retriever import LocalRetriever
from sql_ai_copilot.knowledge.structured_knowledge import StructuredKnowledgeBase
from sql_ai_copilot.knowledge.vector_store import KnowledgeVectorStore
from sql_ai_copilot.llm.ollama_client import OllamaClient
from sql_ai_copilot.llm.openai_compatible_client import OpenAICompatibleClient
from sql_ai_copilot.logging_utils import get_logger
from sql_ai_copilot.security.router import SecurityRouter
from sql_ai_copilot.semantic import DeterministicSQLPlanner, SemanticAnalyzer
from sql_ai_copilot.sql_meta import SQL_ENGINE_LABELS, TASK_MODE_LABELS, is_query_task, normalize_sql_engine, normalize_task_mode


DOCUMENT_CATEGORY_LABELS = {
    "schema_docs": "表结构",
    "metrics": "指标口径",
    "business_rules": "业务规则",
    "sql_examples": "SQL样例",
    "structured_metrics": "结构化指标",
    "structured_dimensions": "结构化维度",
    "structured_synonyms": "同义词词典",
    "structured_relationships": "表关系图谱",
    "structured_fields": "字段业务释义",
    "structured_examples": "结构化样例",
}
DOCUMENT_TITLE_LABELS = {
    "user": "用户",
    "order_main": "订单主表",
    "order_item": "订单明细表",
    "refund_main": "退款主表",
    "refund_item": "退款明细表",
    "product": "商品",
    "store": "门店",
    "warehouse": "仓库",
    "inventory_snapshot": "库存快照表",
    "inventory_flow": "库存流水表",
    "gmv": "GMV口径",
    "order_count": "订单量口径",
    "refund_rate": "退款率口径",
    "avg_order_amount": "客单价口径",
    "repeat_purchase_rate": "复购率口径",
    "fulfillment": "履约口径",
    "inventory_qty": "库存量口径",
    "inventory_movement": "库存流转口径",
    "order_rules": "订单规则",
    "refund_rules": "退款规则",
    "demo_data_rules": "演示数据规则",
    "ecommerce_semantics": "电商语义规则",
    "inventory_rules": "库存规则",
    "product_refund_rules": "商品退款规则",
    "gmv_by_channel": "渠道GMV样例",
    "gmv_by_series": "系列GMV样例",
    "channel_gmv_refund_rate": "渠道GMV退款率样例",
    "repeat_purchase_by_channel": "渠道复购样例",
    "fulfillment_by_channel": "渠道履约样例",
    "gmv_mom_by_channel": "渠道GMV环比样例",
    "inventory_by_warehouse": "仓库库存样例",
    "inventory_flow_by_series": "系列库存流水样例",
}


@lru_cache(maxsize=1)
def _get_knowledge_base() -> StructuredKnowledgeBase:
    settings = get_settings()
    return StructuredKnowledgeBase(settings.knowledge_dir)


@lru_cache(maxsize=1)
def _load_knowledge_documents():
    settings = get_settings()
    raw_documents = KnowledgeLoader(settings.knowledge_dir).load()
    structured_documents = _get_knowledge_base().as_documents()
    return tuple([*raw_documents, *structured_documents])


@lru_cache(maxsize=1)
def _get_vector_store() -> KnowledgeVectorStore:
    settings = get_settings()
    vector_store = KnowledgeVectorStore(LocalEmbeddingModel(settings.embedding), settings.embedding.index_dir)
    vector_store.ensure_index(list(_load_knowledge_documents()))
    return vector_store


@lru_cache(maxsize=1)
def _get_retriever() -> LocalRetriever:
    return LocalRetriever(list(_load_knowledge_documents()), vector_store=_get_vector_store(), reranker=LocalReranker())


@lru_cache(maxsize=1)
def _embedding_info() -> dict[str, object]:
    settings = get_settings()
    meta_path = settings.embedding.index_dir / "knowledge_vectors.json"
    doc_count = 0
    if meta_path.exists():
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
            doc_count = len(payload.get("signatures", {}))
        except Exception:
            doc_count = 0
    return {
        "enabled": settings.embedding.enabled,
        "backend": settings.embedding.backend,
        "model_name": settings.embedding.model_name or settings.embedding.base_url,
        "disabled_reason": None if settings.embedding.enabled else "本地 embedding 已关闭。",
        "doc_count": doc_count,
    }


@lru_cache(maxsize=1)
def _get_semantic_analyzer() -> SemanticAnalyzer:
    return SemanticAnalyzer(_get_knowledge_base())


@lru_cache(maxsize=1)
def _get_planner() -> DeterministicSQLPlanner:
    return DeterministicSQLPlanner()


@lru_cache(maxsize=1)
def _get_llm_client() -> OpenAICompatibleClient:
    return OpenAICompatibleClient(get_settings())


@lru_cache(maxsize=1)
def _get_local_llm_client() -> OllamaClient:
    settings = get_settings()
    return OllamaClient(settings.local_llm.base_url, settings.local_llm.default_model)


def build_agent(client: MySQLClient) -> SQLCopilot:
    settings = get_settings()
    schema_service = SchemaService(client, settings.mysql.database)
    retriever = _get_retriever()
    validator = SQLValidator(client)
    llm_client = _get_llm_client()
    local_llm_client = _get_local_llm_client()
    semantic_analyzer = _get_semantic_analyzer()
    planner = _get_planner()
    knowledge_base = _get_knowledge_base()
    security_router = SecurityRouter(knowledge_base)
    return SQLCopilot(client, schema_service, retriever, validator, llm_client, local_llm_client, semantic_analyzer, planner, security_router, knowledge_base)


def create_app() -> Flask:
    settings = get_settings()
    logger = get_logger("app")
    app = Flask(
        __name__,
        template_folder=str(settings.web_dir / "templates"),
        static_folder=str(settings.web_dir / "static"),
    )

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            app_title="凯尔本地测试",
            default_provider=settings.default_provider,
            app_port=settings.app_port,
        )

    @app.get("/api/models")
    def get_models():
        local_client = _get_local_llm_client()
        catalog = {
            provider_name: {
                "label": provider.label,
                "default_model": provider.default_model,
                "model_options": list(provider.model_options),
            }
            for provider_name, provider in settings.providers.items()
        }
        catalog["local"] = {
            "label": "本地模型",
            "default_model": settings.local_llm.default_model,
            "model_options": local_client.list_models(),
        }
        return jsonify(
            {
                "default_provider": settings.default_provider,
                "providers": catalog,
                "task_modes": [{"value": key, "label": value} for key, value in TASK_MODE_LABELS.items()],
                "sql_engines": [{"value": key, "label": value} for key, value in SQL_ENGINE_LABELS.items()],
                "engine_modes": [
                    {"value": "single", "label": "单引擎"},
                    {"value": "dual", "label": "双引擎"},
                ],
                "embedding": _embedding_info(),
                "local_llm": {
                    "base_url": settings.local_llm.base_url,
                    "default_model": settings.local_llm.default_model,
                    "model_options": local_client.list_models(),
                },
            }
        )

    @app.get("/api/schema")
    def get_schema():
        try:
            with MySQLClient(settings.mysql) as client:
                schema_service = SchemaService(client, settings.mysql.database)
                summary = schema_service.get_schema_summary()
                try:
                    checks = client.query((settings.sql_dir / "sanity_checks.sql").read_text(encoding="utf-8"))
                except Exception:
                    checks = []
            return jsonify({"schema": summary, "checks": checks})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.post("/api/query")
    def query_sql():
        payload = request.get_json(silent=True) or {}
        question = (payload.get("question") or "").strip()
        provider_name = payload.get("provider") or settings.default_provider
        engine_mode = payload.get("engine_mode") or "single"
        local_model_name = payload.get("local_model") or settings.local_llm.default_model
        online_model_name = payload.get("online_model") or payload.get("model") or ""
        execute = bool(payload.get("execute", True))
        requested_task_mode = payload.get("task_mode")
        requested_sql_engine = payload.get("sql_engine") or "mysql"
        if not question:
            return jsonify({"error": "问题不能为空。"}), 400
        try:
            if provider_name == "local":
                model_name = payload.get("model") or local_model_name or settings.local_llm.default_model
            else:
                model_name = payload.get("model") or settings.get_provider(provider_name).default_model
            normalized_task_mode = normalize_task_mode(requested_task_mode)
            normalized_sql_engine = normalize_sql_engine(requested_sql_engine)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        logger.info(
            "api_query_request %s",
            json.dumps(
                {
                    "question": question,
                    "provider": provider_name,
                    "model": model_name,
                    "engine_mode": engine_mode,
                    "local_model": local_model_name,
                    "online_model": online_model_name,
                    "execute": execute,
                    "task_mode": normalized_task_mode,
                    "sql_engine": normalized_sql_engine,
                },
                ensure_ascii=False,
            ),
        )

        try:
            with MySQLClient(settings.mysql) as client:
                agent = build_agent(client)
                result = agent.run(
                    question,
                    provider_name,
                    model_name,
                    execute=execute,
                    task_mode=normalized_task_mode,
                    sql_engine=normalized_sql_engine,
                    engine_mode=engine_mode,
                    local_model_name=local_model_name,
                    online_model_name=online_model_name,
                )
            logger.info(
                "api_query_success %s",
                json.dumps({"question": question, "message": result.message, "trace": result.trace}, ensure_ascii=False),
            )
            return jsonify(
                {
                    "sql": result.sql,
                    "rows": result.rows or [],
                    "documents": [
                        serialize_document(document)
                        for document in result.documents
                    ],
                    "trace": result.trace,
                    "message": result.message,
                    "task_mode": result.task_mode,
                    "sql_engine": result.sql_engine,
                    "has_result_set": result.has_result_set,
                    "executed": result.executed,
                }
            )
        except SQLClarificationRequired as exc:
            logger.warning(
                "api_query_clarification %s",
                json.dumps(
                    {"question": question, "message": exc.message, "fields": exc.fields, "choices": exc.choices, "trace": exc.trace},
                    ensure_ascii=False,
                ),
            )
            return (
                jsonify(
                    {
                        "error": exc.message,
                        "clarification_required": True,
                        "clarification_fields": exc.fields,
                        "clarification_choices": exc.choices,
                        "documents": [serialize_document(document) for document in exc.documents],
                        "trace": exc.trace,
                        "task_mode": exc.task_mode,
                        "sql_engine": exc.sql_engine,
                        "has_result_set": is_query_task(exc.task_mode),
                        "executed": False,
                    }
                ),
                409,
            )
        except SQLCopilotError as exc:
            logger.warning("api_query_failed %s", json.dumps({"question": question, "error": exc.message, "trace": exc.trace}, ensure_ascii=False))
            return (
                jsonify(
                    {
                        "error": exc.message,
                        "sql": exc.sql,
                        "documents": [
                            serialize_document(document)
                            for document in exc.documents
                        ],
                        "trace": exc.trace,
                        "task_mode": exc.task_mode,
                        "sql_engine": exc.sql_engine,
                        "has_result_set": exc.has_result_set,
                        "executed": exc.executed,
                    }
                ),
                400,
            )
        except Exception as exc:
            logger.exception("api_query_unexpected_error")
            return jsonify({"error": str(exc)}), 500

    return app


def serialize_document(document) -> dict[str, str]:
    return {
        "title": DOCUMENT_TITLE_LABELS.get(document.title, document.title),
        "category": DOCUMENT_CATEGORY_LABELS.get(document.category, document.category),
        "path": str(document.path),
    }


app = create_app()


if __name__ == "__main__":
    settings = get_settings()
    app.run(host=settings.app_host, port=settings.app_port, debug=False)
