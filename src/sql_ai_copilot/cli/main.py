from __future__ import annotations

import argparse

from tabulate import tabulate

from sql_ai_copilot.agent.sql_copilot import SQLCopilot
from sql_ai_copilot.config.settings import get_settings
from sql_ai_copilot.database.demo_seed import DemoDataSeeder, SeedConfig
from sql_ai_copilot.database.mysql_client import MySQLClient
from sql_ai_copilot.database.schema_service import SchemaService
from sql_ai_copilot.database.sql_validator import SQLValidator
from sql_ai_copilot.knowledge.embedding import LocalEmbeddingModel
from sql_ai_copilot.knowledge.document_loader import KnowledgeLoader
from sql_ai_copilot.knowledge.retriever import LocalRetriever
from sql_ai_copilot.knowledge.vector_store import KnowledgeVectorStore
from sql_ai_copilot.llm.openai_compatible_client import OpenAICompatibleClient
from sql_ai_copilot.semantic import DeterministicSQLPlanner, SemanticAnalyzer


def build_agent(client: MySQLClient) -> SQLCopilot:
    settings = get_settings()
    schema_service = SchemaService(client, settings.mysql.database)
    documents = KnowledgeLoader(settings.knowledge_dir).load()
    vector_store = KnowledgeVectorStore(LocalEmbeddingModel(settings.embedding), settings.embedding.index_dir)
    vector_store.ensure_index(documents)
    retriever = LocalRetriever(documents, vector_store=vector_store)
    validator = SQLValidator(client)
    llm_client = OpenAICompatibleClient(settings)
    return SQLCopilot(client, schema_service, retriever, validator, llm_client, SemanticAnalyzer(), DeterministicSQLPlanner())


def main() -> None:
    parser = argparse.ArgumentParser(description="Local SQL AI Copilot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db", help="创建数据库并生成演示数据")
    init_parser.add_argument("--users", type=int, default=None)
    init_parser.add_argument("--orders", type=int, default=None)
    init_parser.add_argument("--refunds", type=int, default=None)

    init_inventory_parser = subparsers.add_parser("init-inventory", help="仅创建并生成库存演示数据")

    ask_parser = subparsers.add_parser("ask", help="生成 SQL 并可选择执行")
    ask_parser.add_argument("question", help="自然语言问题")
    ask_parser.add_argument("--provider", default=None, help="bailian 或 deepseek")
    ask_parser.add_argument("--model", default="", help="覆盖默认模型")
    ask_parser.add_argument("--no-execute", action="store_true", help="只生成 SQL，不执行")
    ask_parser.add_argument("--task-mode", default=None, help="auto/dql/ads_sql/ddl/dml/dcl")
    ask_parser.add_argument("--sql-engine", default="mysql", help="mysql/hql/pg/oracle/odpssql/sqlserver")

    schema_parser = subparsers.add_parser("schema", help="输出数据库 Schema 摘要")
    schema_parser.add_argument("--check", action="store_true", help="输出行数校验")

    args = parser.parse_args()
    settings = get_settings()

    with MySQLClient(settings.mysql) as client:
        if args.command == "init-db":
            seed_config = SeedConfig(
                user_count=args.users or settings.seed.user_count,
                order_count=args.orders or settings.seed.order_count,
                refund_count=args.refunds or settings.seed.refund_count,
                batch_size=settings.seed.batch_size,
                random_seed=settings.seed.random_seed,
            )
            seeder = DemoDataSeeder(client, settings.sql_dir / "create_tables.sql", seed_config, settings.mysql.database)
            seeder.run()
            checks = client.query((settings.sql_dir / "sanity_checks.sql").read_text(encoding="utf-8"))
            print(tabulate(checks, headers="keys", tablefmt="github"))
            return

        if args.command == "init-inventory":
            seed_config = SeedConfig(
                user_count=settings.seed.user_count,
                order_count=settings.seed.order_count,
                refund_count=settings.seed.refund_count,
                batch_size=settings.seed.batch_size,
                random_seed=settings.seed.random_seed,
            )
            seeder = DemoDataSeeder(client, settings.sql_dir / "create_tables.sql", seed_config, settings.mysql.database)
            seeder.seed_inventory_only(settings.sql_dir / "create_inventory_tables.sql")
            checks = client.query(
                """
                SELECT 'dim_warehouse' AS table_name, COUNT(*) AS row_count FROM dim_warehouse
                UNION ALL
                SELECT 'fct_inventory_snapshot' AS table_name, COUNT(*) AS row_count FROM fct_inventory_snapshot
                UNION ALL
                SELECT 'fct_inventory_flow' AS table_name, COUNT(*) AS row_count FROM fct_inventory_flow
                ORDER BY table_name
                """
            )
            print(tabulate(checks, headers="keys", tablefmt="github"))
            return

        if args.command == "schema":
            schema_service = SchemaService(client, settings.mysql.database)
            print(schema_service.get_schema_summary())
            if args.check:
                checks = client.query((settings.sql_dir / "sanity_checks.sql").read_text(encoding="utf-8"))
                print(tabulate(checks, headers="keys", tablefmt="github"))
            return

        if args.command == "ask":
            agent = build_agent(client)
            provider = args.provider or settings.default_provider
            result = agent.run(
                args.question,
                provider,
                args.model,
                execute=not args.no_execute,
                task_mode=args.task_mode,
                sql_engine=args.sql_engine,
            )
            print(result.sql)
            if result.rows is not None:
                print(tabulate(result.rows[:50], headers="keys", tablefmt="github"))


if __name__ == "__main__":
    main()
