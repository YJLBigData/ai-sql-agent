SELECT
    ds.channel_name,
    SUM(om.payment_amount) AS gmv,
    COUNT(DISTINCT om.order_id) AS order_cnt
FROM fct_order_main om
JOIN dim_store ds
  ON om.store_id = ds.store_id
WHERE om.order_date >= CURDATE() - INTERVAL 30 DAY
  AND om.pay_status = '已支付'
GROUP BY ds.channel_name
ORDER BY gmv DESC;

