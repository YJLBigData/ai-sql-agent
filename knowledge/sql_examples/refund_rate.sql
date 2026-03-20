SELECT
    ds.channel_name,
    COUNT(DISTINCT CASE WHEN om.pay_status = '已支付' THEN om.order_id END) AS paid_order_cnt,
    COUNT(DISTINCT CASE WHEN rm.refund_status = '退款成功' THEN rm.order_id END) AS success_refund_order_cnt,
    ROUND(
        COUNT(DISTINCT CASE WHEN rm.refund_status = '退款成功' THEN rm.order_id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN om.pay_status = '已支付' THEN om.order_id END), 0),
        4
    ) AS refund_rate
FROM fct_order_main om
JOIN dim_store ds
  ON om.store_id = ds.store_id
LEFT JOIN fct_refund_main rm
  ON om.order_id = rm.order_id
WHERE om.order_date >= CURDATE() - INTERVAL 30 DAY
GROUP BY ds.channel_name
ORDER BY refund_rate DESC;
