# 履约口径

- 指标名:
  - `fulfillment_rate`: 履约率
  - `avg_fulfillment_hours`: 平均履约时长
- 默认公式:
  - 履约率: `已完成订单数 / 已支付订单数`
  - 平均履约时长: `AVG(TIMESTAMPDIFF(HOUR, pay_time, finish_time))`
- 过滤条件:
  - 默认限定 `pay_status = '已支付'`
- 说明:
  - 当前演示库没有专门的物流轨迹表，履约分析基于 `order_status`、`pay_time`、`finish_time` 做轻量测算。
