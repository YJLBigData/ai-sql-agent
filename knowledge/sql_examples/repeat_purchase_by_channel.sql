WITH user_order_agg AS (
    SELECT
        ds.channel_name,
        om.user_id,
        COUNT(DISTINCT om.order_id) AS order_count
    FROM fct_order_main om
    JOIN dim_store ds
      ON om.store_id = ds.store_id
    WHERE om.pay_status = '已支付'
      AND om.order_date >= CURDATE() - INTERVAL 30 DAY
    GROUP BY ds.channel_name, om.user_id
)
SELECT
    channel_name,
    SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) AS repeat_user_count,
    ROUND(SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT user_id), 0), 4) AS repeat_purchase_rate
FROM user_order_agg
GROUP BY channel_name
ORDER BY repeat_purchase_rate DESC;
