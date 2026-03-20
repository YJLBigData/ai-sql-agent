# fct_refund_main

- 表名: `fct_refund_main`
- 粒度: 一行一笔退款单
- 主键: `refund_id`
- 常用字段:
  - `refund_date`: 退款申请日期
  - `refund_status`: 退款状态，分析退款金额时优先取 `退款成功`
  - `refund_type`: `整单退款` 或 `部分退款`
  - `refund_reason`: 退款原因
  - `refund_amount`: 退款金额
  - `order_id`: 原订单 ID
- 典型用途:
  - 退款金额、退款率、退款原因分析

