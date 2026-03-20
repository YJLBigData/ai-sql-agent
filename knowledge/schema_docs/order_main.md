# fct_order_main

- 表名: `fct_order_main`
- 粒度: 一行一笔订单
- 主键: `order_id`
- 常用字段:
  - `order_date` / `order_time`: 下单日期、下单时间
  - `order_status`: 订单状态
  - `pay_status`: 支付状态
  - `payment_amount`: 订单实付金额，GMV 默认取该字段
  - `refund_amount`: 成功退款金额汇总
  - `net_payment_amount`: 净销售额，等于 `payment_amount - refund_amount`
  - `order_source`: 订单来源渠道
  - `store_id`: 门店
  - `user_id`: 用户
- 典型关联:
  - `fct_order_main.user_id = dim_user.user_id`
  - `fct_order_main.store_id = dim_store.store_id`
  - `fct_order_main.order_id = fct_order_item.order_id`

