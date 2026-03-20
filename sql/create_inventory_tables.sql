SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS fct_inventory_flow;
DROP TABLE IF EXISTS fct_inventory_snapshot;
DROP TABLE IF EXISTS dim_warehouse;

SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE dim_warehouse (
    warehouse_id INT PRIMARY KEY COMMENT '仓库ID',
    warehouse_code VARCHAR(32) NOT NULL COMMENT '仓库编码',
    warehouse_name VARCHAR(128) NOT NULL COMMENT '仓库名称',
    warehouse_type VARCHAR(16) COMMENT '仓库类型',
    region_name VARCHAR(16) COMMENT '大区',
    province_name VARCHAR(16) COMMENT '省份',
    city_name VARCHAR(16) COMMENT '城市',
    service_channel VARCHAR(16) COMMENT '主要服务渠道',
    is_active TINYINT(1) COMMENT '是否有效',
    open_date DATE COMMENT '启用日期',
    KEY idx_wh_region (region_name, province_name, city_name),
    KEY idx_wh_type (warehouse_type),
    KEY idx_service_channel (service_channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='仓库维表';

CREATE TABLE fct_inventory_snapshot (
    snapshot_id BIGINT PRIMARY KEY COMMENT '库存快照ID',
    snapshot_date DATE NOT NULL COMMENT '快照日期',
    warehouse_id INT NOT NULL COMMENT '仓库ID',
    product_id INT NOT NULL COMMENT '商品ID',
    brand_name VARCHAR(32) NOT NULL COMMENT '品牌名称',
    series_name VARCHAR(32) NOT NULL COMMENT '系列名称',
    category_name VARCHAR(32) NOT NULL COMMENT '类目名称',
    product_name VARCHAR(128) NOT NULL COMMENT '商品名称',
    inventory_qty INT NOT NULL COMMENT '账面库存量',
    reserved_qty INT NOT NULL COMMENT '预占库存量',
    available_qty INT NOT NULL COMMENT '可用库存量',
    inbound_qty_7d INT NOT NULL COMMENT '近7天入库量',
    outbound_qty_7d INT NOT NULL COMMENT '近7天出库量',
    safety_stock_qty INT NOT NULL COMMENT '安全库存量',
    stock_status VARCHAR(16) NOT NULL COMMENT '库存状态',
    unit_cost DECIMAL(18, 2) COMMENT '单位成本',
    inventory_amount DECIMAL(18, 2) COMMENT '库存金额',
    KEY idx_snapshot_date (snapshot_date),
    KEY idx_snapshot_wh (warehouse_id, snapshot_date),
    KEY idx_snapshot_product (product_id, snapshot_date),
    KEY idx_snapshot_series (series_name, snapshot_date),
    KEY idx_stock_status (stock_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='库存快照事实表';

CREATE TABLE fct_inventory_flow (
    flow_id BIGINT PRIMARY KEY COMMENT '库存流水ID',
    flow_no VARCHAR(32) NOT NULL COMMENT '库存流水号',
    warehouse_id INT NOT NULL COMMENT '仓库ID',
    product_id INT NOT NULL COMMENT '商品ID',
    brand_name VARCHAR(32) NOT NULL COMMENT '品牌名称',
    series_name VARCHAR(32) NOT NULL COMMENT '系列名称',
    category_name VARCHAR(32) NOT NULL COMMENT '类目名称',
    product_name VARCHAR(128) NOT NULL COMMENT '商品名称',
    flow_date DATE NOT NULL COMMENT '业务日期',
    flow_time DATETIME NOT NULL COMMENT '业务时间',
    flow_type VARCHAR(16) NOT NULL COMMENT '流水类型',
    direction VARCHAR(8) NOT NULL COMMENT '方向',
    quantity INT NOT NULL COMMENT '数量',
    unit_cost DECIMAL(18, 2) COMMENT '单位成本',
    amount DECIMAL(18, 2) COMMENT '金额',
    remark VARCHAR(128) COMMENT '备注',
    KEY idx_flow_date (flow_date),
    KEY idx_flow_wh (warehouse_id, flow_date),
    KEY idx_flow_product (product_id, flow_date),
    KEY idx_flow_series (series_name, flow_date),
    KEY idx_flow_direction (direction, flow_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='库存流水事实表';
