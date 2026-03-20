SELECT
    dw.warehouse_name,
    SUM(s.inventory_qty) AS inventory_qty,
    SUM(s.available_qty) AS available_qty,
    ROUND(
        SUM(CASE WHEN s.available_qty <= 0 OR s.stock_status = '缺货' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(DISTINCT s.product_id), 0),
        4
    ) AS stockout_rate
FROM fct_inventory_snapshot s
JOIN dim_warehouse dw
  ON s.warehouse_id = dw.warehouse_id
WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM fct_inventory_snapshot)
GROUP BY dw.warehouse_name
ORDER BY inventory_qty DESC;
