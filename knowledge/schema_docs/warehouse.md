# dim_warehouse

- 表名: `dim_warehouse`
- 粒度: 一行一个仓库
- 主键: `warehouse_id`
- 常用字段:
  - `warehouse_name`: 仓库名称
  - `warehouse_type`: 仓库类型，如中央仓、区域仓、前置仓
  - `region_name` / `province_name` / `city_name`: 仓库地域
  - `service_channel`: 主要服务渠道
- 典型用途:
  - 仓库维度库存分析
  - 仓库维度入库/出库分析
