# 库存量口径

- 指标名:
  - `inventory_qty`: 账面库存量
  - `available_qty`: 可用库存量
  - `stockout_rate`: 缺货率
- 默认口径:
  - 库存量 = `SUM(inventory_qty)`
  - 可用库存 = `SUM(available_qty)`
  - 缺货率 = `缺货SKU数 / SKU总数`
  - SKU总数默认取 `COUNT(DISTINCT product_id)`
- 默认时间:
  - 若用户未指定日期，默认使用最新库存快照日
