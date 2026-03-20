from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from random import Random

from .mysql_client import MySQLClient
from .schema_service import SchemaService


PRODUCT_SERIES = {
    "特仑苏": [
        ("有机纯牛奶 250ml*12", "250ml*12", 79.90, 56.20, "常温纯牛奶"),
        ("纯牛奶 250ml*16", "250ml*16", 88.00, 61.60, "常温纯牛奶"),
        ("低脂牛奶 250ml*12", "250ml*12", 72.00, 50.40, "低脂牛奶"),
        ("梦幻盖纯牛奶 250ml*10", "250ml*10", 69.90, 48.00, "常温纯牛奶"),
        ("沙漠有机纯牛奶 250ml*10", "250ml*10", 75.00, 53.50, "常温纯牛奶"),
        ("A2β-酪蛋白纯牛奶 250ml*10", "250ml*10", 99.00, 69.30, "功能奶"),
    ],
    "纯甄": [
        ("经典风味酸牛奶 200g*12", "200g*12", 49.90, 33.50, "常温酸奶"),
        ("轻食黄桃燕麦酸奶 200g*10", "200g*10", 45.90, 30.70, "常温酸奶"),
        ("小蛮腰原味酸奶 230g*10", "230g*10", 52.00, 35.90, "常温酸奶"),
        ("高蛋白酸奶 200g*12", "200g*12", 59.00, 41.30, "高蛋白酸奶"),
        ("红西柚口味酸奶 200g*10", "200g*10", 43.00, 29.50, "常温酸奶"),
        ("蓝莓果粒酸奶 200g*10", "200g*10", 44.50, 30.10, "常温酸奶"),
    ],
    "每日鲜语": [
        ("鲜牛奶 250ml*10", "250ml*10", 69.90, 49.80, "低温鲜奶"),
        ("0乳糖鲜牛奶 250ml*10", "250ml*10", 79.00, 56.50, "低温鲜奶"),
        ("鲜牛奶 950ml", "950ml", 18.90, 13.20, "低温鲜奶"),
        ("鲜牛奶 450ml", "450ml", 11.90, 8.10, "低温鲜奶"),
        ("高钙鲜牛奶 250ml*10", "250ml*10", 72.00, 51.20, "低温鲜奶"),
        ("A2鲜牛奶 250ml*10", "250ml*10", 88.00, 62.80, "低温鲜奶"),
    ],
    "优益C": [
        ("乳酸菌饮品 原味 100ml*20", "100ml*20", 36.90, 24.80, "乳酸菌"),
        ("乳酸菌饮品 柠檬味 100ml*20", "100ml*20", 36.90, 24.80, "乳酸菌"),
        ("乳酸菌饮品 草莓味 100ml*20", "100ml*20", 38.90, 26.30, "乳酸菌"),
        ("小蓝瓶活菌型 340ml*10", "340ml*10", 49.90, 34.20, "乳酸菌"),
        ("活菌型原味 100ml*16", "100ml*16", 32.00, 21.40, "乳酸菌"),
        ("活菌型青提味 100ml*16", "100ml*16", 32.00, 21.40, "乳酸菌"),
    ],
    "未来星": [
        ("儿童成长牛奶 骨力型 190ml*15", "190ml*15", 58.00, 39.50, "儿童奶"),
        ("儿童成长牛奶 智慧型 190ml*15", "190ml*15", 58.00, 39.50, "儿童奶"),
        ("双原生纯牛奶 190ml*12", "190ml*12", 52.00, 35.30, "儿童奶"),
        ("儿童有机纯牛奶 190ml*12", "190ml*12", 62.00, 42.60, "儿童奶"),
        ("学生高钙奶 190ml*15", "190ml*15", 56.00, 38.40, "儿童奶"),
        ("学生早餐奶 190ml*15", "190ml*15", 54.00, 36.80, "儿童奶"),
    ],
    "冠益乳": [
        ("BB-12原味发酵乳 250g*10", "250g*10", 65.00, 45.20, "低温酸奶"),
        ("双歧杆菌风味发酵乳 250g*10", "250g*10", 63.00, 43.80, "低温酸奶"),
        ("果粒发酵乳 蓝莓味 230g*10", "230g*10", 61.00, 42.10, "低温酸奶"),
        ("零蔗糖风味发酵乳 230g*10", "230g*10", 66.00, 46.00, "低温酸奶"),
        ("高蛋白发酵乳 230g*10", "230g*10", 68.00, 47.50, "低温酸奶"),
        ("芦荟果粒发酵乳 230g*10", "230g*10", 60.00, 41.70, "低温酸奶"),
    ],
    "随变": [
        ("香草冰淇淋 65g*10", "65g*10", 42.00, 29.50, "冰淇淋"),
        ("巧克力脆皮冰淇淋 75g*10", "75g*10", 46.00, 32.30, "冰淇淋"),
        ("生椰拿铁冰淇淋 70g*10", "70g*10", 52.00, 36.80, "冰淇淋"),
        ("白桃乌龙冰淇淋 70g*10", "70g*10", 50.00, 35.40, "冰淇淋"),
        ("轻乳雪糕 70g*10", "70g*10", 48.00, 33.60, "冰淇淋"),
        ("榴莲冰淇淋 70g*8", "70g*8", 56.00, 39.90, "冰淇淋"),
    ],
    "奶特": [
        ("咖啡牛奶 250ml*12", "250ml*12", 39.90, 27.10, "调制乳"),
        ("麦香牛奶 250ml*12", "250ml*12", 39.90, 27.10, "调制乳"),
        ("巧克力牛奶 250ml*12", "250ml*12", 41.90, 28.50, "调制乳"),
        ("早餐奶 原味 250ml*12", "250ml*12", 37.90, 25.80, "早餐奶"),
        ("谷物牛奶 250ml*12", "250ml*12", 42.90, 29.00, "调制乳"),
        ("红枣牛奶 250ml*12", "250ml*12", 40.90, 27.80, "调制乳"),
    ],
}

CITY_GROUPS = {
    "华北": [("北京", "北京"), ("天津", "天津"), ("河北", "石家庄"), ("山西", "太原")],
    "华东": [("上海", "上海"), ("江苏", "南京"), ("浙江", "杭州"), ("山东", "济南"), ("安徽", "合肥")],
    "华南": [("广东", "广州"), ("福建", "厦门"), ("广西", "南宁")],
    "华中": [("河南", "郑州"), ("湖北", "武汉"), ("湖南", "长沙"), ("江西", "南昌")],
    "西南": [("四川", "成都"), ("重庆", "重庆"), ("云南", "昆明"), ("贵州", "贵阳")],
    "西北": [("陕西", "西安"), ("甘肃", "兰州"), ("宁夏", "银川"), ("新疆", "乌鲁木齐")],
    "东北": [("辽宁", "沈阳"), ("吉林", "长春"), ("黑龙江", "哈尔滨")],
}

STORE_CHANNELS = [("天猫", "旗舰店"), ("京东", "自营店"), ("抖音", "旗舰店"), ("拼多多", "旗舰店")]
SOURCE_CHANNELS = ["天猫", "京东", "抖音", "拼多多", "私域"]
PAYMENT_TYPES = ["支付宝", "微信支付", "银行卡", "云闪付"]
REFUND_REASONS = ["商品破损", "临期/日期不满意", "拍错/多拍", "物流时效问题", "口味不符预期"]
WAREHOUSE_TYPES = ["中央仓", "区域仓", "前置仓"]
INVENTORY_FLOW_TYPES = [
    ("采购入库", "入库"),
    ("退货入库", "入库"),
    ("调拨入库", "入库"),
    ("销售出库", "出库"),
    ("调拨出库", "出库"),
    ("盘亏出库", "出库"),
]


def money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def dt_to_str(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else None


@dataclass(frozen=True)
class SeedConfig:
    user_count: int
    order_count: int
    refund_count: int
    batch_size: int
    random_seed: int


class DemoDataSeeder:
    def __init__(self, client: MySQLClient, ddl_file: Path, config: SeedConfig, database_name: str) -> None:
        self.client = client
        self.ddl_file = ddl_file
        self.config = config
        self.schema_service = SchemaService(client, database_name)
        self.rng = Random(config.random_seed)
        self.products = self._build_products()
        self.stores = self._build_stores()
        self.warehouses = self._build_warehouses()

    def run(self) -> None:
        self.schema_service.create_tables_from_file(self.ddl_file)
        self._truncate_tables()
        self._seed_products()
        self._seed_stores()
        self._seed_warehouses()
        self._seed_users()
        paid_order_refs = self._seed_orders_and_items()
        self._seed_refunds(paid_order_refs)
        self._refresh_refund_metrics()
        self._seed_inventory()

    def seed_inventory_only(self, inventory_ddl_file: Path) -> None:
        self.client.execute_script(inventory_ddl_file.read_text(encoding="utf-8"))
        self._truncate_inventory_tables()
        self._seed_warehouses()
        self._seed_inventory()

    def _build_products(self) -> list[dict[str, object]]:
        products: list[dict[str, object]] = []
        product_id = 1
        for series_name, items in PRODUCT_SERIES.items():
            for item_index, (item_name, package_spec, list_price, cost_price, category_name) in enumerate(items, start=1):
                products.append(
                    {
                        "product_id": product_id,
                        "spu_code": f"SPU{product_id:05d}",
                        "sku_code": f"SKU{product_id:05d}",
                        "brand_name": "蒙牛",
                        "series_name": series_name,
                        "category_name": category_name,
                        "product_name": f"蒙牛{series_name}{item_name}",
                        "package_spec": package_spec,
                        "unit": "件",
                        "list_price": money(list_price),
                        "cost_price": money(cost_price),
                        "is_active": 1,
                    }
                )
                product_id += 1
        return products

    def _build_stores(self) -> list[dict[str, object]]:
        stores: list[dict[str, object]] = []
        store_id = 1
        for region_name, locations in CITY_GROUPS.items():
            for province_name, city_name in locations:
                for channel_name, store_type in STORE_CHANNELS:
                    stores.append(
                        {
                            "store_id": store_id,
                            "store_code": f"STORE{store_id:04d}",
                            "store_name": f"蒙牛{channel_name}{city_name}{store_type}",
                            "channel_name": channel_name,
                            "store_type": store_type,
                            "region_name": region_name,
                            "province_name": province_name,
                            "city_name": city_name,
                            "open_date": date(2021 + (store_id % 3), ((store_id % 12) + 1), ((store_id % 27) + 1)),
                        }
                    )
                    store_id += 1
        return stores

    def _build_warehouses(self) -> list[dict[str, object]]:
        warehouses: list[dict[str, object]] = []
        warehouse_id = 1
        for region_name, locations in CITY_GROUPS.items():
            province_name, city_name = locations[0]
            warehouses.append(
                {
                    "warehouse_id": warehouse_id,
                    "warehouse_code": f"WH{warehouse_id:03d}",
                    "warehouse_name": f"蒙牛{region_name}中央仓",
                    "warehouse_type": "中央仓",
                    "region_name": region_name,
                    "province_name": province_name,
                    "city_name": city_name,
                    "service_channel": self.rng.choice(["全渠道", "天猫", "京东", "抖音", "拼多多"]),
                    "is_active": 1,
                    "open_date": date(2021 + (warehouse_id % 3), ((warehouse_id % 12) + 1), ((warehouse_id % 27) + 1)),
                }
            )
            warehouse_id += 1
            warehouses.append(
                {
                    "warehouse_id": warehouse_id,
                    "warehouse_code": f"WH{warehouse_id:03d}",
                    "warehouse_name": f"蒙牛{city_name}前置仓",
                    "warehouse_type": "前置仓",
                    "region_name": region_name,
                    "province_name": province_name,
                    "city_name": city_name,
                    "service_channel": self.rng.choice(["天猫", "京东", "抖音", "拼多多"]),
                    "is_active": 1,
                    "open_date": date(2022 + (warehouse_id % 2), ((warehouse_id % 12) + 1), ((warehouse_id % 27) + 1)),
                }
            )
            warehouse_id += 1
        return warehouses

    def _truncate_tables(self) -> None:
        self.client.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table_name in (
            "fct_inventory_flow",
            "fct_inventory_snapshot",
            "fct_refund_item",
            "fct_refund_main",
            "fct_order_item",
            "fct_order_main",
            "dim_warehouse",
            "dim_product",
            "dim_store",
            "dim_user",
        ):
            self.client.execute(f"TRUNCATE TABLE {table_name}")
        self.client.execute("SET FOREIGN_KEY_CHECKS = 1")

    def _truncate_inventory_tables(self) -> None:
        self.client.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table_name in ("fct_inventory_flow", "fct_inventory_snapshot", "dim_warehouse"):
            self.client.execute(f"TRUNCATE TABLE {table_name}")
        self.client.execute("SET FOREIGN_KEY_CHECKS = 1")

    def _seed_products(self) -> None:
        sql = """
        INSERT INTO dim_product (
            product_id, spu_code, sku_code, brand_name, series_name, category_name,
            product_name, package_spec, unit, list_price, cost_price, is_active
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                item["product_id"],
                item["spu_code"],
                item["sku_code"],
                item["brand_name"],
                item["series_name"],
                item["category_name"],
                item["product_name"],
                item["package_spec"],
                item["unit"],
                item["list_price"],
                item["cost_price"],
                item["is_active"],
            )
            for item in self.products
        ]
        self.client.executemany(sql, rows)

    def _seed_stores(self) -> None:
        sql = """
        INSERT INTO dim_store (
            store_id, store_code, store_name, channel_name, store_type,
            region_name, province_name, city_name, open_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                store["store_id"],
                store["store_code"],
                store["store_name"],
                store["channel_name"],
                store["store_type"],
                store["region_name"],
                store["province_name"],
                store["city_name"],
                store["open_date"],
            )
            for store in self.stores
        ]
        self.client.executemany(sql, rows)

    def _seed_warehouses(self) -> None:
        sql = """
        INSERT INTO dim_warehouse (
            warehouse_id, warehouse_code, warehouse_name, warehouse_type,
            region_name, province_name, city_name, service_channel, is_active, open_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                warehouse["warehouse_id"],
                warehouse["warehouse_code"],
                warehouse["warehouse_name"],
                warehouse["warehouse_type"],
                warehouse["region_name"],
                warehouse["province_name"],
                warehouse["city_name"],
                warehouse["service_channel"],
                warehouse["is_active"],
                warehouse["open_date"],
            )
            for warehouse in self.warehouses
        ]
        self.client.executemany(sql, rows)

    def _seed_users(self) -> None:
        sql = """
        INSERT INTO dim_user (
            user_id, user_code, user_name, gender, age, member_level,
            register_date, region_name, province_name, city_name,
            source_channel, is_member, mobile
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        geo_pool = [
            (region_name, province_name, city_name)
            for region_name, locations in CITY_GROUPS.items()
            for province_name, city_name in locations
        ]
        user_start_date = date.today() - timedelta(days=430)

        batch: list[tuple[object, ...]] = []
        for user_id in range(1, self.config.user_count + 1):
            region_name, province_name, city_name = geo_pool[(user_id - 1) % len(geo_pool)]
            register_date = user_start_date + timedelta(days=self.rng.randint(0, 430))
            source_channel = self.rng.choices(SOURCE_CHANNELS, weights=[30, 25, 17, 14, 14], k=1)[0]
            member_level = "普通"
            if user_id % 20 == 0:
                member_level = "钻石"
            elif user_id % 10 in (0, 1, 2):
                member_level = "金卡"
            elif user_id % 10 in (3, 4, 5, 6):
                member_level = "银卡"
            batch.append(
                (
                    user_id,
                    f"U{user_id:08d}",
                    f"蒙牛消费者{user_id:06d}",
                    "女" if user_id % 2 == 0 else "男",
                    18 + (user_id % 38),
                    member_level,
                    register_date,
                    region_name,
                    province_name,
                    city_name,
                    source_channel,
                    1 if user_id % 10 < 8 else 0,
                    f"13{100000000 + ((user_id * 37) % 900000000):09d}",
                )
            )
            if len(batch) >= self.config.batch_size:
                self.client.executemany(sql, batch)
                batch.clear()
        if batch:
            self.client.executemany(sql, batch)

    def _seed_orders_and_items(self) -> list[dict[str, object]]:
        order_sql = """
        INSERT INTO fct_order_main (
            order_id, order_no, user_id, store_id, order_date, order_time,
            order_status, pay_status, payment_type, order_source,
            total_item_qty, total_sku_count, origin_amount, discount_amount,
            shipping_fee, payment_amount, refund_amount, net_payment_amount,
            pay_time, finish_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        item_sql = """
        INSERT INTO fct_order_item (
            order_item_id, order_id, line_no, product_id, product_name, brand_name,
            series_name, category_name, quantity, origin_unit_price, pay_unit_price,
            origin_amount, pay_amount, cost_amount, refunded_amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        order_rows: list[tuple[object, ...]] = []
        item_rows: list[tuple[object, ...]] = []
        paid_orders: list[dict[str, object]] = []
        order_item_id = 1
        start_dt = datetime.combine(date.today() - timedelta(days=430), time.min)

        for order_id in range(1, self.config.order_count + 1):
            user_id = self.rng.randint(1, self.config.user_count)
            store = self.rng.choice(self.stores)
            order_time = start_dt + timedelta(
                days=self.rng.randint(0, 430),
                hours=self.rng.randint(0, 23),
                minutes=self.rng.randint(0, 59),
            )
            order_date = order_time.date()
            order_source = self.rng.choices(SOURCE_CHANNELS, weights=[32, 26, 18, 14, 10], k=1)[0]
            payment_type = self.rng.choice(PAYMENT_TYPES)
            status_flag = self.rng.random()
            if status_flag < 0.68:
                order_status = "已完成"
            elif status_flag < 0.90:
                order_status = "待收货"
            else:
                order_status = "已取消"

            item_count = self.rng.randint(1, 3)
            origin_amount_total = Decimal("0.00")
            pay_amount_total = Decimal("0.00")
            total_item_qty = 0
            first_paid_item: dict[str, object] | None = None

            for line_no in range(1, item_count + 1):
                product = self.rng.choice(self.products)
                quantity = self.rng.randint(1, 3)
                discount_rate = self.rng.uniform(0.0, 0.18)
                origin_unit_price = product["list_price"]
                pay_unit_price = Decimal("0.00") if order_status == "已取消" else money(float(product["list_price"]) * (1 - discount_rate))
                origin_amount = money(float(product["list_price"]) * quantity)
                pay_amount = Decimal("0.00") if order_status == "已取消" else money(float(pay_unit_price) * quantity)
                cost_amount = money(float(product["cost_price"]) * quantity)

                origin_amount_total += origin_amount
                pay_amount_total += pay_amount
                total_item_qty += quantity

                item_rows.append(
                    (
                        order_item_id,
                        order_id,
                        line_no,
                        product["product_id"],
                        product["product_name"],
                        product["brand_name"],
                        product["series_name"],
                        product["category_name"],
                        quantity,
                        origin_unit_price,
                        pay_unit_price,
                        origin_amount,
                        pay_amount,
                        cost_amount,
                        Decimal("0.00"),
                    )
                )
                if pay_amount > 0 and first_paid_item is None:
                    first_paid_item = {
                        "order_item_id": order_item_id,
                        "line_no": line_no,
                        "product_id": product["product_id"],
                        "product_name": product["product_name"],
                        "brand_name": product["brand_name"],
                        "series_name": product["series_name"],
                        "category_name": product["category_name"],
                        "quantity": quantity,
                        "pay_unit_price": pay_unit_price,
                        "pay_amount": pay_amount,
                    }
                order_item_id += 1

            shipping_fee = Decimal("0.00")
            if order_status != "已取消" and pay_amount_total < Decimal("79.00"):
                shipping_fee = Decimal("8.00")
            payment_amount = Decimal("0.00") if order_status == "已取消" else pay_amount_total + shipping_fee
            pay_time = None if order_status == "已取消" else order_time + timedelta(minutes=self.rng.randint(1, 30))
            finish_time = order_time + timedelta(days=self.rng.randint(2, 9)) if order_status == "已完成" else None
            discount_amount = origin_amount_total - pay_amount_total

            order_rows.append(
                (
                    order_id,
                    f"MN{order_date.strftime('%Y%m%d')}{order_id:08d}",
                    user_id,
                    store["store_id"],
                    order_date,
                    dt_to_str(order_time),
                    order_status,
                    "未支付" if order_status == "已取消" else "已支付",
                    payment_type,
                    order_source,
                    total_item_qty,
                    item_count,
                    origin_amount_total,
                    discount_amount,
                    shipping_fee,
                    payment_amount,
                    Decimal("0.00"),
                    payment_amount,
                    dt_to_str(pay_time),
                    dt_to_str(finish_time),
                )
            )

            if payment_amount > 0 and first_paid_item is not None:
                paid_orders.append(
                    {
                        "order_id": order_id,
                        "user_id": user_id,
                        "store_id": store["store_id"],
                        "order_time": order_time,
                        "payment_amount": payment_amount,
                        "first_item": first_paid_item,
                    }
                )

            if len(order_rows) >= self.config.batch_size:
                self.client.executemany(order_sql, order_rows)
                self.client.executemany(item_sql, item_rows)
                order_rows.clear()
                item_rows.clear()

        if order_rows:
            self.client.executemany(order_sql, order_rows)
        if item_rows:
            self.client.executemany(item_sql, item_rows)
        return paid_orders

    def _seed_refunds(self, paid_orders: list[dict[str, object]]) -> None:
        refund_sql = """
        INSERT INTO fct_refund_main (
            refund_id, refund_no, order_id, user_id, store_id, refund_date,
            refund_apply_time, refund_finish_time, refund_status, refund_type,
            refund_reason, refund_amount, refund_item_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        refund_item_sql = """
        INSERT INTO fct_refund_item (
            refund_item_id, refund_id, order_id, order_item_id, line_no, product_id,
            product_name, brand_name, series_name, category_name, refund_quantity,
            refund_unit_amount, refund_amount, refund_item_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        selected_orders = self.rng.sample(paid_orders, k=min(self.config.refund_count, len(paid_orders)))
        refund_rows: list[tuple[object, ...]] = []
        refund_item_rows: list[tuple[object, ...]] = []

        for refund_id, order_ref in enumerate(selected_orders, start=1):
            item = order_ref["first_item"]
            quantity = item["quantity"]
            refund_quantity = 2 if quantity >= 2 and self.rng.random() < 0.25 else 1
            refund_quantity = min(refund_quantity, quantity)
            refund_amount = money(float(item["pay_unit_price"]) * refund_quantity)
            refund_apply_time = order_ref["order_time"] + timedelta(days=self.rng.randint(1, 20), hours=self.rng.randint(0, 12))
            status_flag = self.rng.random()
            if status_flag < 0.88:
                refund_status = "退款成功"
                refund_finish_time = refund_apply_time + timedelta(days=self.rng.randint(1, 5))
            elif status_flag < 0.96:
                refund_status = "退款处理中"
                refund_finish_time = None
            else:
                refund_status = "退款关闭"
                refund_finish_time = None
            refund_type = "整单退款" if refund_amount >= order_ref["payment_amount"] else "部分退款"
            refund_reason = self.rng.choice(REFUND_REASONS)
            refund_rows.append(
                (
                    refund_id,
                    f"RF{refund_apply_time.strftime('%Y%m%d')}{refund_id:08d}",
                    order_ref["order_id"],
                    order_ref["user_id"],
                    order_ref["store_id"],
                    refund_apply_time.date(),
                    dt_to_str(refund_apply_time),
                    dt_to_str(refund_finish_time),
                    refund_status,
                    refund_type,
                    refund_reason,
                    refund_amount,
                    1,
                )
            )
            refund_item_rows.append(
                (
                    refund_id,
                    refund_id,
                    order_ref["order_id"],
                    item["order_item_id"],
                    item["line_no"],
                    item["product_id"],
                    item["product_name"],
                    item["brand_name"],
                    item["series_name"],
                    item["category_name"],
                    refund_quantity,
                    item["pay_unit_price"],
                    refund_amount,
                    refund_status if refund_status != "退款关闭" else "退款关闭",
                )
            )

        self.client.executemany(refund_sql, refund_rows)
        self.client.executemany(refund_item_sql, refund_item_rows)

    def _refresh_refund_metrics(self) -> None:
        self.client.execute(
            """
            UPDATE fct_order_item oi
            JOIN (
                SELECT order_item_id, ROUND(SUM(refund_amount), 2) AS refund_amount
                FROM fct_refund_item
                WHERE refund_item_status = '退款成功'
                GROUP BY order_item_id
            ) r
              ON oi.order_item_id = r.order_item_id
            SET oi.refunded_amount = r.refund_amount
            """
        )

    def _seed_inventory(self) -> None:
        snapshot_sql = """
        INSERT INTO fct_inventory_snapshot (
            snapshot_id, snapshot_date, warehouse_id, product_id, brand_name, series_name,
            category_name, product_name, inventory_qty, reserved_qty, available_qty,
            inbound_qty_7d, outbound_qty_7d, safety_stock_qty, stock_status,
            unit_cost, inventory_amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        flow_sql = """
        INSERT INTO fct_inventory_flow (
            flow_id, flow_no, warehouse_id, product_id, brand_name, series_name, category_name,
            product_name, flow_date, flow_time, flow_type, direction, quantity, unit_cost, amount, remark
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        snapshot_rows: list[tuple[object, ...]] = []
        flow_rows: list[tuple[object, ...]] = []
        snapshot_date = date.today() - timedelta(days=1)
        start_dt = datetime.combine(snapshot_date - timedelta(days=44), time(8, 0))
        snapshot_id = 1
        flow_id = 1

        for warehouse in self.warehouses:
            warehouse_bias = 1.3 if warehouse["warehouse_type"] == "中央仓" else 0.65
            for product in self.products:
                demand_base = 18 + (product["product_id"] % 11) * 6
                outbound_qty_7d = max(5, int(demand_base * warehouse_bias * self.rng.uniform(0.8, 1.35)))
                inbound_qty_7d = max(4, int(outbound_qty_7d * self.rng.uniform(0.85, 1.18)))
                safety_stock_qty = max(12, int(outbound_qty_7d * self.rng.uniform(0.7, 1.35)))
                inventory_qty = max(0, int((outbound_qty_7d + safety_stock_qty) * self.rng.uniform(0.8, 2.6)))
                reserved_qty = min(inventory_qty, int(max(0, outbound_qty_7d * self.rng.uniform(0.05, 0.3))))
                available_qty = max(inventory_qty - reserved_qty, 0)
                if available_qty <= 0:
                    stock_status = "缺货"
                elif available_qty < safety_stock_qty:
                    stock_status = "紧张"
                elif available_qty > safety_stock_qty * 3:
                    stock_status = "积压"
                else:
                    stock_status = "正常"
                unit_cost = product["cost_price"]
                inventory_amount = money(float(unit_cost) * available_qty)
                snapshot_rows.append(
                    (
                        snapshot_id,
                        snapshot_date,
                        warehouse["warehouse_id"],
                        product["product_id"],
                        product["brand_name"],
                        product["series_name"],
                        product["category_name"],
                        product["product_name"],
                        inventory_qty,
                        reserved_qty,
                        available_qty,
                        inbound_qty_7d,
                        outbound_qty_7d,
                        safety_stock_qty,
                        stock_status,
                        unit_cost,
                        inventory_amount,
                    )
                )
                snapshot_id += 1

                flow_days = 14
                for day_offset in range(flow_days):
                    flow_day = snapshot_date - timedelta(days=flow_days - day_offset - 1)
                    daily_outbound = max(0, int(outbound_qty_7d / 7 * self.rng.uniform(0.55, 1.45)))
                    daily_inbound = max(0, int(inbound_qty_7d / 7 * self.rng.uniform(0.55, 1.45)))
                    for quantity, flow_type, direction in (
                        (daily_inbound, "采购入库", "入库"),
                        (daily_outbound, "销售出库", "出库"),
                    ):
                        if quantity <= 0:
                            continue
                        flow_time = start_dt + timedelta(days=day_offset, hours=self.rng.randint(0, 10), minutes=self.rng.randint(0, 59))
                        amount = money(float(unit_cost) * quantity)
                        flow_rows.append(
                            (
                                flow_id,
                                f"IV{flow_day.strftime('%Y%m%d')}{flow_id:08d}",
                                warehouse["warehouse_id"],
                                product["product_id"],
                                product["brand_name"],
                                product["series_name"],
                                product["category_name"],
                                product["product_name"],
                                flow_day,
                                dt_to_str(flow_time),
                                flow_type,
                                direction,
                                quantity,
                                unit_cost,
                                amount,
                                f"{warehouse['warehouse_name']}{flow_type}",
                            )
                        )
                        flow_id += 1
                    if day_offset % 5 == 0:
                        adjust_qty = max(1, int((daily_outbound + daily_inbound + 1) * self.rng.uniform(0.06, 0.18)))
                        flow_type, direction = self.rng.choice(INVENTORY_FLOW_TYPES[1:])
                        amount = money(float(unit_cost) * adjust_qty)
                        flow_time = start_dt + timedelta(days=day_offset, hours=self.rng.randint(11, 20), minutes=self.rng.randint(0, 59))
                        flow_rows.append(
                            (
                                flow_id,
                                f"IV{flow_day.strftime('%Y%m%d')}{flow_id:08d}",
                                warehouse["warehouse_id"],
                                product["product_id"],
                                product["brand_name"],
                                product["series_name"],
                                product["category_name"],
                                product["product_name"],
                                flow_day,
                                dt_to_str(flow_time),
                                flow_type,
                                direction,
                                adjust_qty,
                                unit_cost,
                                amount,
                                f"{warehouse['warehouse_name']}{flow_type}",
                            )
                        )
                        flow_id += 1

                if len(snapshot_rows) >= self.config.batch_size:
                    self.client.executemany(snapshot_sql, snapshot_rows)
                    snapshot_rows.clear()
                if len(flow_rows) >= self.config.batch_size * 3:
                    self.client.executemany(flow_sql, flow_rows)
                    flow_rows.clear()

        if snapshot_rows:
            self.client.executemany(snapshot_sql, snapshot_rows)
        if flow_rows:
            self.client.executemany(flow_sql, flow_rows)
        self.client.execute(
            """
            UPDATE fct_order_main om
            JOIN (
                SELECT order_id, ROUND(SUM(refund_amount), 2) AS refund_amount
                FROM fct_refund_main
                WHERE refund_status = '退款成功'
                GROUP BY order_id
            ) r
              ON om.order_id = r.order_id
            SET om.refund_amount = r.refund_amount,
                om.net_payment_amount = ROUND(om.payment_amount - r.refund_amount, 2),
                om.order_status = CASE
                    WHEN r.refund_amount >= om.payment_amount THEN '全额退款'
                    WHEN r.refund_amount > 0 THEN '部分退款'
                    ELSE om.order_status
                END
            """
        )
