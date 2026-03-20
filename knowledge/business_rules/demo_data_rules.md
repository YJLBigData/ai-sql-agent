# 演示数据规则

- 当前演示数据库中的商品品牌全部为 `蒙牛`。
- 用户如果只提“蒙牛”，默认就是全量商品，不需要额外关联 `fct_order_item` 或 `dim_product` 做品牌过滤。
- 统计渠道维度 GMV、退款金额、退款率时，优先从 `fct_order_main` 和 `fct_refund_main` 分别聚合后再关联 `dim_store`。

