# GMV 口径

- 默认 GMV 字段: `fct_order_main.payment_amount`
- 统计条件:
  - 默认按已支付订单口径统计，即 `pay_status = '已支付'`
  - 如果用户没特别说明，不剔除部分退款订单
- 不建议直接汇总 `fct_order_item.pay_amount` 做订单 GMV，因为主表已经含运费

