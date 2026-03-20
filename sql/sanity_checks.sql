SELECT 'dim_product' AS table_name, COUNT(*) AS row_count FROM dim_product
UNION ALL
SELECT 'dim_store' AS table_name, COUNT(*) AS row_count FROM dim_store
UNION ALL
SELECT 'dim_warehouse' AS table_name, COUNT(*) AS row_count FROM dim_warehouse
UNION ALL
SELECT 'dim_user' AS table_name, COUNT(*) AS row_count FROM dim_user
UNION ALL
SELECT 'fct_inventory_flow' AS table_name, COUNT(*) AS row_count FROM fct_inventory_flow
UNION ALL
SELECT 'fct_inventory_snapshot' AS table_name, COUNT(*) AS row_count FROM fct_inventory_snapshot
UNION ALL
SELECT 'fct_order_item' AS table_name, COUNT(*) AS row_count FROM fct_order_item
UNION ALL
SELECT 'fct_order_main' AS table_name, COUNT(*) AS row_count FROM fct_order_main
UNION ALL
SELECT 'fct_refund_item' AS table_name, COUNT(*) AS row_count FROM fct_refund_item
UNION ALL
SELECT 'fct_refund_main' AS table_name, COUNT(*) AS row_count FROM fct_refund_main
ORDER BY table_name;
