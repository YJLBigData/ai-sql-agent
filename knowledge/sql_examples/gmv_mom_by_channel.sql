WITH current_agg AS (
    SELECT
        ds.channel_name,
        SUM(om.payment_amount) AS current_gmv
    FROM fct_order_main om
    JOIN dim_store ds
      ON om.store_id = ds.store_id
    WHERE om.pay_status = '已支付'
      AND om.order_date >= CURDATE() - INTERVAL 30 DAY
    GROUP BY ds.channel_name
),
previous_agg AS (
    SELECT
        ds.channel_name,
        SUM(om.payment_amount) AS previous_gmv
    FROM fct_order_main om
    JOIN dim_store ds
      ON om.store_id = ds.store_id
    WHERE om.pay_status = '已支付'
      AND om.order_date >= CURDATE() - INTERVAL 60 DAY
      AND om.order_date < CURDATE() - INTERVAL 30 DAY
    GROUP BY ds.channel_name
)
SELECT
    COALESCE(ca.channel_name, pa.channel_name) AS channel_name,
    COALESCE(ca.current_gmv, 0) AS current_gmv,
    COALESCE(pa.previous_gmv, 0) AS previous_gmv,
    ROUND((COALESCE(ca.current_gmv, 0) - COALESCE(pa.previous_gmv, 0)) / NULLIF(COALESCE(pa.previous_gmv, 0), 0), 4) AS mom_rate
FROM current_agg ca
LEFT JOIN previous_agg pa
  ON ca.channel_name = pa.channel_name
ORDER BY current_gmv DESC;
