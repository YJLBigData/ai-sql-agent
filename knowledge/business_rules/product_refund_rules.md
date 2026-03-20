# 商品退款规则

- 如果问题按 `系列`、`类目`、`商品` 统计退款金额或退款率，应优先使用 `fct_refund_item`。
- 如果问题按 `系列`、`类目`、`商品` 统计 GMV 或订单量，应优先使用 `fct_order_item` 联 `fct_order_main`。
- 商品粒度分析不要直接拿 `fct_order_main.payment_amount` 去分摊到商品，否则会造成口径错误。
- 商品退款率默认口径：
  - 分子：`fct_refund_item` 对应维度下的成功退款订单数
  - 分母：`fct_order_item` 对应维度下的已支付订单数
