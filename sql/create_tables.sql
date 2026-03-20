SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS fct_inventory_flow;
DROP TABLE IF EXISTS fct_inventory_snapshot;
DROP TABLE IF EXISTS dim_warehouse;
DROP TABLE IF EXISTS fct_refund_item;
DROP TABLE IF EXISTS fct_refund_main;
DROP TABLE IF EXISTS fct_order_item;
DROP TABLE IF EXISTS fct_order_main;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_user;

SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE dim_user (
    user_id BIGINT PRIMARY KEY COMMENT '用户ID',
    user_code VARCHAR(32) NOT NULL COMMENT '用户编码',
    user_name VARCHAR(64) NOT NULL COMMENT '用户名称',
    gender VARCHAR(8) COMMENT '性别',
    age INT COMMENT '年龄',
    member_level VARCHAR(16) COMMENT '会员等级',
    register_date DATE COMMENT '注册日期',
    region_name VARCHAR(16) COMMENT '大区',
    province_name VARCHAR(16) COMMENT '省份',
    city_name VARCHAR(16) COMMENT '城市',
    source_channel VARCHAR(16) COMMENT '来源渠道',
    is_member TINYINT(1) COMMENT '是否会员',
    mobile VARCHAR(16) COMMENT '手机号',
    KEY idx_register_date (register_date),
    KEY idx_region_city (region_name, city_name),
    KEY idx_source_channel (source_channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户维表';

CREATE TABLE dim_product (
    product_id INT PRIMARY KEY COMMENT '商品ID',
    spu_code VARCHAR(32) NOT NULL COMMENT 'SPU编码',
    sku_code VARCHAR(32) NOT NULL COMMENT 'SKU编码',
    brand_name VARCHAR(32) NOT NULL COMMENT '品牌名称',
    series_name VARCHAR(32) NOT NULL COMMENT '系列名称',
    category_name VARCHAR(32) NOT NULL COMMENT '类目名称',
    product_name VARCHAR(128) NOT NULL COMMENT '商品名称',
    package_spec VARCHAR(64) COMMENT '包装规格',
    unit VARCHAR(16) COMMENT '单位',
    list_price DECIMAL(18, 2) COMMENT '吊牌价',
    cost_price DECIMAL(18, 2) COMMENT '成本价',
    is_active TINYINT(1) COMMENT '是否有效',
    KEY idx_series_name (series_name),
    KEY idx_category_name (category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品维表';

CREATE TABLE dim_store (
    store_id INT PRIMARY KEY COMMENT '门店ID',
    store_code VARCHAR(32) NOT NULL COMMENT '门店编码',
    store_name VARCHAR(128) NOT NULL COMMENT '门店名称',
    channel_name VARCHAR(16) NOT NULL COMMENT '渠道名称',
    store_type VARCHAR(16) COMMENT '门店类型',
    region_name VARCHAR(16) COMMENT '大区',
    province_name VARCHAR(16) COMMENT '省份',
    city_name VARCHAR(16) COMMENT '城市',
    open_date DATE COMMENT '开店日期',
    KEY idx_channel_region (channel_name, region_name),
    KEY idx_province_city (province_name, city_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='门店维表';

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

CREATE TABLE fct_order_main (
    order_id BIGINT PRIMARY KEY COMMENT '订单ID',
    order_no VARCHAR(32) NOT NULL COMMENT '订单号',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    store_id INT NOT NULL COMMENT '门店ID',
    order_date DATE NOT NULL COMMENT '下单日期',
    order_time DATETIME NOT NULL COMMENT '下单时间',
    order_status VARCHAR(16) NOT NULL COMMENT '订单状态',
    pay_status VARCHAR(16) NOT NULL COMMENT '支付状态',
    payment_type VARCHAR(16) COMMENT '支付方式',
    order_source VARCHAR(16) COMMENT '订单来源',
    total_item_qty INT COMMENT '总购买件数',
    total_sku_count INT COMMENT '总SKU行数',
    origin_amount DECIMAL(18, 2) COMMENT '原价金额',
    discount_amount DECIMAL(18, 2) COMMENT '优惠金额',
    shipping_fee DECIMAL(18, 2) COMMENT '运费',
    payment_amount DECIMAL(18, 2) COMMENT '实付金额',
    refund_amount DECIMAL(18, 2) COMMENT '成功退款金额',
    net_payment_amount DECIMAL(18, 2) COMMENT '净销售额',
    pay_time DATETIME COMMENT '支付时间',
    finish_time DATETIME COMMENT '完成时间',
    KEY idx_order_date (order_date),
    KEY idx_user_id (user_id),
    KEY idx_store_id (store_id),
    KEY idx_order_status (order_status),
    KEY idx_order_source (order_source),
    KEY idx_order_date_store (order_date, store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='订单主表';

CREATE TABLE fct_order_item (
    order_item_id BIGINT PRIMARY KEY COMMENT '订单明细ID',
    order_id BIGINT NOT NULL COMMENT '订单ID',
    line_no INT NOT NULL COMMENT '行号',
    product_id INT NOT NULL COMMENT '商品ID',
    product_name VARCHAR(128) NOT NULL COMMENT '商品名称',
    brand_name VARCHAR(32) NOT NULL COMMENT '品牌名称',
    series_name VARCHAR(32) NOT NULL COMMENT '系列名称',
    category_name VARCHAR(32) NOT NULL COMMENT '类目名称',
    quantity INT NOT NULL COMMENT '购买数量',
    origin_unit_price DECIMAL(18, 2) COMMENT '原价单价',
    pay_unit_price DECIMAL(18, 2) COMMENT '实付单价',
    origin_amount DECIMAL(18, 2) COMMENT '原价金额',
    pay_amount DECIMAL(18, 2) COMMENT '实付金额',
    cost_amount DECIMAL(18, 2) COMMENT '成本金额',
    refunded_amount DECIMAL(18, 2) COMMENT '成功退款金额',
    KEY idx_order_id (order_id),
    KEY idx_product_id (product_id),
    KEY idx_series_name (series_name),
    KEY idx_category_name (category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='订单明细表';

CREATE TABLE fct_refund_main (
    refund_id BIGINT PRIMARY KEY COMMENT '退款单ID',
    refund_no VARCHAR(32) NOT NULL COMMENT '退款单号',
    order_id BIGINT NOT NULL COMMENT '订单ID',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    store_id INT NOT NULL COMMENT '门店ID',
    refund_date DATE NOT NULL COMMENT '退款申请日期',
    refund_apply_time DATETIME NOT NULL COMMENT '退款申请时间',
    refund_finish_time DATETIME COMMENT '退款完成时间',
    refund_status VARCHAR(16) NOT NULL COMMENT '退款状态',
    refund_type VARCHAR(16) NOT NULL COMMENT '退款类型',
    refund_reason VARCHAR(32) NOT NULL COMMENT '退款原因',
    refund_amount DECIMAL(18, 2) COMMENT '退款金额',
    refund_item_count INT COMMENT '退款商品行数',
    KEY idx_refund_date (refund_date),
    KEY idx_refund_status (refund_status),
    KEY idx_order_id (order_id),
    KEY idx_store_id (store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='退款主表';

CREATE TABLE fct_refund_item (
    refund_item_id BIGINT PRIMARY KEY COMMENT '退款明细ID',
    refund_id BIGINT NOT NULL COMMENT '退款单ID',
    order_id BIGINT NOT NULL COMMENT '订单ID',
    order_item_id BIGINT NOT NULL COMMENT '订单明细ID',
    line_no INT NOT NULL COMMENT '订单行号',
    product_id INT NOT NULL COMMENT '商品ID',
    product_name VARCHAR(128) NOT NULL COMMENT '商品名称',
    brand_name VARCHAR(32) NOT NULL COMMENT '品牌名称',
    series_name VARCHAR(32) NOT NULL COMMENT '系列名称',
    category_name VARCHAR(32) NOT NULL COMMENT '类目名称',
    refund_quantity INT NOT NULL COMMENT '退款数量',
    refund_unit_amount DECIMAL(18, 2) COMMENT '退款单价',
    refund_amount DECIMAL(18, 2) COMMENT '退款金额',
    refund_item_status VARCHAR(16) NOT NULL COMMENT '退款明细状态',
    KEY idx_refund_id (refund_id),
    KEY idx_order_item_id (order_item_id),
    KEY idx_product_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='退款明细表';

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
