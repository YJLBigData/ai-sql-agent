# 订单量口径

- 默认订单量: `COUNT(DISTINCT fct_order_main.order_id)`
- 已支付订单量:
  - `COUNT(DISTINCT CASE WHEN pay_status = '已支付' THEN order_id END)`
- 成功退款订单量:
  - `COUNT(DISTINCT CASE WHEN refund_status = '退款成功' THEN order_id END)`

