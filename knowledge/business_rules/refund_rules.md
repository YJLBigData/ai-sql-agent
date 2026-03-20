# 退款业务规则

- 退款金额分析优先使用 `fct_refund_main`。
- 商品级退款分析优先使用 `fct_refund_item`。
- 退款状态默认只看 `退款成功`，除非用户显式要求看处理中或关闭单。
- 订单净销售额优先使用 `fct_order_main.net_payment_amount`。

