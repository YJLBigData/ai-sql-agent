WITH order_agg AS (
    SELECT
        ds.channel_name,
        SUM(om.payment_amount) AS gmv,
        COUNT(DISTINCT om.order_id) AS paid_order_cnt
    FROM fct_order_main om
    JOIN dim_store ds
      ON om.store_id = ds.store_id
    WHERE om.pay_status = '已支付'
      AND om.order_date >= CURDATE() - INTERVAL 30 DAY
    GROUP BY ds.channel_name
),
refund_agg AS (
    SELECT
        ds.channel_name,
        SUM(rm.refund_amount) AS refund_amount,
        COUNT(DISTINCT rm.order_id) AS refund_order_cnt
    FROM fct_refund_main rm
    JOIN dim_store ds
      ON rm.store_id = ds.store_id
    WHERE rm.refund_status = '退款成功'
      AND rm.refund_date >= CURDATE() - INTERVAL 30 DAY
    GROUP BY ds.channel_name
)
SELECT
    oa.channel_name,
    oa.gmv,
    COALESCE(ra.refund_amount, 0) AS refund_amount,
    ROUND(COALESCE(ra.refund_order_cnt, 0) / NULLIF(oa.paid_order_cnt, 0), 4) AS refund_rate
FROM order_agg oa
LEFT JOIN refund_agg ra
  ON oa.channel_name = ra.channel_name
ORDER BY oa.gmv DESC;
