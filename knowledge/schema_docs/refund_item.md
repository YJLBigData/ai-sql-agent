# fct_refund_item

- 表名: `fct_refund_item`
- 粒度: 一行一笔退款商品明细
- 主键: `refund_item_id`
- 常用字段:
  - `refund_id`: 退款主单 ID
  - `order_item_id`: 订单商品明细 ID
  - `product_id`: 商品 ID
  - `refund_quantity`: 退款件数
  - `refund_amount`: 明细退款金额
  - `refund_item_status`: 明细退款状态
- 典型用途:
  - 商品级退款分析
  - 退款商品 Top N 排行

