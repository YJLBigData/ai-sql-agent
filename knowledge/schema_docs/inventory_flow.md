# fct_inventory_flow

- 表名: `fct_inventory_flow`
- 粒度: 一行是一笔库存流水
- 主键: `flow_id`
- 常用字段:
  - `flow_date` / `flow_time`: 业务日期、业务时间
  - `warehouse_id`: 仓库ID
  - `product_id`: 商品ID
  - `flow_type`: 流水类型
  - `direction`: 入库 / 出库
  - `quantity`: 数量
  - `amount`: 金额
- 典型用途:
  - 入库量、出库量分析
  - 仓库/系列/商品维度库存流水分析
