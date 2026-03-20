# 库存流转口径

- 指标名:
  - `inbound_qty`: 入库量
  - `outbound_qty`: 出库量
- 默认口径:
  - 入库量 = `SUM(CASE WHEN direction = '入库' THEN quantity END)`
  - 出库量 = `SUM(CASE WHEN direction = '出库' THEN quantity END)`
- 默认时间:
  - 按 `flow_date` 过滤
  - 未指定时间范围时，默认统计当前库内已有流水数据
