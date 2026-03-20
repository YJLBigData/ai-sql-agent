# fct_inventory_snapshot

- 表名: `fct_inventory_snapshot`
- 粒度: 一行是某日某仓某商品的库存快照
- 主键: `snapshot_id`
- 常用字段:
  - `snapshot_date`: 快照日期
  - `warehouse_id`: 仓库ID
  - `product_id`: 商品ID
  - `inventory_qty`: 账面库存量
  - `reserved_qty`: 预占库存量
  - `available_qty`: 可用库存量
  - `inbound_qty_7d` / `outbound_qty_7d`: 近7天入库量、出库量
  - `safety_stock_qty`: 安全库存量
  - `stock_status`: 库存状态
- 典型关联:
  - `fct_inventory_snapshot.warehouse_id = dim_warehouse.warehouse_id`
  - `fct_inventory_snapshot.product_id = dim_product.product_id`
