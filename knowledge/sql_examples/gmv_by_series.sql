SELECT
    oi.series_name,
    SUM(oi.pay_amount) AS item_gmv,
    SUM(oi.refunded_amount) AS refunded_amount,
    SUM(oi.pay_amount - oi.refunded_amount) AS net_item_gmv
FROM fct_order_item oi
JOIN fct_order_main om
  ON oi.order_id = om.order_id
WHERE om.order_date BETWEEN '2025-01-01' AND '2025-01-31'
  AND om.pay_status = '已支付'
GROUP BY oi.series_name
ORDER BY item_gmv DESC;

