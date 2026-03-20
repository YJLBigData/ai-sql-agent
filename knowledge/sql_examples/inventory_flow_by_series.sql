SELECT
    flow.series_name,
    SUM(CASE WHEN flow.direction = '入库' THEN flow.quantity ELSE 0 END) AS inbound_qty,
    SUM(CASE WHEN flow.direction = '出库' THEN flow.quantity ELSE 0 END) AS outbound_qty
FROM fct_inventory_flow flow
WHERE flow.flow_date >= CURDATE() - INTERVAL 30 DAY
GROUP BY flow.series_name
ORDER BY outbound_qty DESC;
