SELECT
    ds.channel_name,
    ROUND(SUM(CASE WHEN om.finish_time IS NOT NULL OR om.order_status = '已完成' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT om.order_id), 0), 4) AS fulfillment_rate,
    ROUND(AVG(CASE WHEN om.pay_time IS NOT NULL AND om.finish_time IS NOT NULL THEN TIMESTAMPDIFF(HOUR, om.pay_time, om.finish_time) END), 2) AS avg_fulfillment_hours
FROM fct_order_main om
JOIN dim_store ds
  ON om.store_id = ds.store_id
WHERE om.pay_status = '已支付'
  AND om.order_date >= CURDATE() - INTERVAL 30 DAY
GROUP BY ds.channel_name
ORDER BY fulfillment_rate DESC;
