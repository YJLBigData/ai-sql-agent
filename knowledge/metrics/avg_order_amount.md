# 客单价口径

- 指标名: `avg_order_amount`
- 中文口径: 客单价
- 默认公式:
  - 门店域: `SUM(fct_order_main.payment_amount) / COUNT(DISTINCT fct_order_main.order_id)`
  - 商品域: `SUM(fct_order_item.pay_amount) / COUNT(DISTINCT fct_order_main.order_id)`
- 过滤条件:
  - 默认限定 `pay_status = '已支付'`
- 说明:
  - 客单价是订单维度指标，分母必须是去重订单数，不能用明细行数或商品件数代替。
