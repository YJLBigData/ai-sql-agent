# fct_order_item

- 表名: `fct_order_item`
- 粒度: 一行一笔订单商品明细
- 主键: `order_item_id`
- 常用字段:
  - `order_id`: 订单主键
  - `product_id`: 商品主键
  - `series_name`: 蒙牛系列，如特仑苏、纯甄、每日鲜语
  - `category_name`: 商品类目
  - `quantity`: 购买件数
  - `pay_amount`: 明细实付金额
  - `refunded_amount`: 成功退款金额
- 典型用途:
  - 商品、品类、系列维度销售分析
  - 和 `dim_product` 关联做商品分析

