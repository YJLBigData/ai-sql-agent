from __future__ import annotations

from dataclasses import asdict

from sql_ai_copilot.config.settings import AppSettings, get_settings
from sql_ai_copilot.database.demo_seed import DemoDataSeeder, SeedConfig
from sql_ai_copilot.database.mysql_client import MySQLClient


TARGET_TABLES = (
    "dim_user",
    "dim_product",
    "dim_store",
    "dim_warehouse",
    "fct_order_main",
    "fct_order_item",
    "fct_refund_main",
    "fct_refund_item",
    "fct_inventory_snapshot",
    "fct_inventory_flow",
)


def _database_ready(client: MySQLClient, database_name: str) -> bool:
    rows = client.query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        """,
        (database_name,),
    )
    existing_tables = {row.get("table_name") or row.get("TABLE_NAME") for row in rows}
    if not set(TARGET_TABLES).issubset(existing_tables):
        return False

    sample_rows = client.query(
        """
        SELECT
            (SELECT COUNT(*) FROM dim_user) AS dim_user_cnt,
            (SELECT COUNT(*) FROM fct_order_main) AS fct_order_main_cnt,
            (SELECT COUNT(*) FROM fct_refund_main) AS fct_refund_main_cnt,
            (SELECT COUNT(*) FROM fct_inventory_snapshot) AS fct_inventory_snapshot_cnt
        """
    )
    sample = sample_rows[0]
    user_count = sample.get("dim_user_cnt") or sample.get("DIM_USER_CNT") or 0
    order_count = sample.get("fct_order_main_cnt") or sample.get("FCT_ORDER_MAIN_CNT") or 0
    refund_count = sample.get("fct_refund_main_cnt") or sample.get("FCT_REFUND_MAIN_CNT") or 0
    inventory_count = sample.get("fct_inventory_snapshot_cnt") or sample.get("FCT_INVENTORY_SNAPSHOT_CNT") or 0
    return user_count > 0 and order_count > 0 and refund_count > 0 and inventory_count > 0


def ensure_demo_database(settings: AppSettings | None = None, force_reseed: bool = False, verbose: bool = False) -> bool:
    settings = settings or get_settings()
    with MySQLClient(settings.mysql) as client:
        ready = False if force_reseed else _database_ready(client, settings.mysql.database)
        if ready:
            if verbose:
                print(f"[sql-agent] MySQL 数据库 {settings.mysql.database} 已就绪，跳过初始化。")
            return False

        if verbose:
            if force_reseed:
                print(f"[sql-agent] 开始重建 MySQL 数据库 {settings.mysql.database} 的演示数据。")
            else:
                print(f"[sql-agent] 检测到数据库或表未初始化，开始生成演示数据到 {settings.mysql.database}。")

        seed_config = SeedConfig(
            user_count=settings.seed.user_count,
            order_count=settings.seed.order_count,
            refund_count=settings.seed.refund_count,
            batch_size=settings.seed.batch_size,
            random_seed=settings.seed.random_seed,
        )
        seeder = DemoDataSeeder(client, settings.sql_dir / "create_tables.sql", seed_config, settings.mysql.database)
        seeder.run()

        if verbose:
            print(
                "[sql-agent] 数据生成完成: "
                + ", ".join(f"{key}={value}" for key, value in asdict(seed_config).items())
            )
        return True
