from __future__ import annotations

import re
from dataclasses import dataclass

from sql_ai_copilot.knowledge.structured_knowledge import StructuredKnowledgeBase

from .models import SemanticContext


@dataclass(frozen=True)
class KeywordSpec:
    name: str
    keywords: tuple[str, ...]


METRIC_SPECS = (
    KeywordSpec("avg_order_amount", ("avg_order_amount", "客单价")),
    KeywordSpec("repeat_purchase_rate", ("repeat_purchase_rate", "复购率")),
    KeywordSpec("repeat_user_count", ("repeat_user_count", "复购人数", "复购用户数", "复购会员数")),
    KeywordSpec("fulfillment_rate", ("fulfillment_rate", "履约率")),
    KeywordSpec("avg_fulfillment_hours", ("avg_fulfillment_hours", "平均履约时长", "履约时长", "履约耗时")),
    KeywordSpec("available_qty", ("available_qty", "可用库存")),
    KeywordSpec("reserved_qty", ("reserved_qty", "预占库存", "锁定库存")),
    KeywordSpec("stockout_rate", ("stockout_rate", "缺货率", "缺货")),
    KeywordSpec("inventory_amount", ("inventory_amount", "库存金额", "库存货值")),
    KeywordSpec("inventory_qty", ("inventory_qty", "库存总量", "总库存", "库存量", "账面库存", "现有库存")),
    KeywordSpec("inbound_qty", ("inbound_qty", "入库量", "入库数")),
    KeywordSpec("outbound_qty", ("outbound_qty", "出库量", "出库数")),
    KeywordSpec("gmv", ("gmv", "销售额", "交易额", "成交额")),
    KeywordSpec("order_count", ("order_count", "订单量", "订单数", "单量")),
    KeywordSpec("refund_amount", ("refund_amount", "退款金额", "退款额")),
    KeywordSpec("refund_rate", ("refund_rate", "退款率")),
    KeywordSpec("net_payment_amount", ("net_payment_amount", "净销售额", "净gmv", "净成交额")),
)

DIMENSION_SPECS = (
    KeywordSpec("channel_name", ("channel_name", "渠道")),
    KeywordSpec("store_name", ("store_name", "门店", "店铺")),
    KeywordSpec("warehouse_name", ("warehouse_name", "仓库")),
    KeywordSpec("warehouse_type", ("warehouse_type", "仓型", "仓库类型")),
    KeywordSpec("region_name", ("region_name", "大区")),
    KeywordSpec("province_name", ("province_name", "省份")),
    KeywordSpec("city_name", ("city_name", "城市")),
    KeywordSpec("series_name", ("series_name", "系列")),
    KeywordSpec("category_name", ("category_name", "类目", "品类")),
    KeywordSpec("brand_name", ("brand_name", "品牌", "品牌名称")),
    KeywordSpec("product_name", ("product_name", "商品", "sku", "产品")),
)

STORE_DIMENSIONS = {"channel_name", "store_name", "region_name", "province_name", "city_name"}
WAREHOUSE_DIMENSIONS = {"warehouse_name", "warehouse_type", "region_name", "province_name", "city_name"}
PRODUCT_DIMENSIONS = {"brand_name", "series_name", "category_name", "product_name"}
INVENTORY_DIMENSIONS = WAREHOUSE_DIMENSIONS | PRODUCT_DIMENSIONS

SUPPORTED_TEMPLATE_METRICS = {
    "gmv",
    "order_count",
    "refund_amount",
    "refund_rate",
    "net_payment_amount",
    "avg_order_amount",
    "repeat_purchase_rate",
    "repeat_user_count",
    "fulfillment_rate",
    "avg_fulfillment_hours",
}
SUPPORTED_INVENTORY_TEMPLATE_METRICS = {
    "inventory_qty",
    "available_qty",
    "reserved_qty",
    "inventory_amount",
    "stockout_rate",
    "inbound_qty",
    "outbound_qty",
}
INVENTORY_SNAPSHOT_METRICS = {"inventory_qty", "available_qty", "reserved_qty", "inventory_amount", "stockout_rate"}
INVENTORY_FLOW_METRICS = {"inbound_qty", "outbound_qty"}
UNSAFE_TEMPLATE_HINTS = (
    "拉新",
    "漏斗",
    "窗口",
    "开窗",
    "排名占比",
    "分群",
    "归因",
)


class SemanticAnalyzer:
    def __init__(self, knowledge_base: StructuredKnowledgeBase | None = None) -> None:
        self.knowledge_base = knowledge_base

    def analyze(self, question: str, task_mode: str, sql_engine: str) -> SemanticContext:
        summary = self.knowledge_base.normalize_question(question) if self.knowledge_base else None
        normalized_question = summary.normalized_question if summary else question
        matched_synonyms = summary.matched_synonyms if summary else ()

        metrics = self._merge_detected_values(
            self._detect_keywords(normalized_question, METRIC_SPECS),
            self.knowledge_base.detect_metric_aliases(normalized_question) if self.knowledge_base else (),
        )
        dimensions = self._merge_detected_values(
            self._detect_keywords(normalized_question, DIMENSION_SPECS),
            self.knowledge_base.detect_dimension_aliases(normalized_question) if self.knowledge_base else (),
        )
        topic = self._detect_topic(normalized_question, metrics)
        metrics = self._normalize_metrics(normalized_question, metrics, topic)
        compare_mode = self._detect_compare_mode(normalized_question)
        time_grain = self._detect_time_grain(normalized_question)
        time_window, time_window_value = self._detect_time_window(normalized_question)
        limit = self._detect_limit(normalized_question)
        sort_metric, sort_desc = self._detect_sort(normalized_question, metrics)
        metric_family = self._detect_metric_family(dimensions, topic)
        requested_tables = self._resolve_tables(normalized_question, metrics, dimensions, metric_family, topic)
        relevant_columns = self._resolve_columns(metrics, dimensions, metric_family, time_grain, topic)
        hints = self._build_hints(metrics, dimensions, time_grain, time_window, metric_family, compare_mode, topic)
        notes = self._build_notes(normalized_question, metrics, dimensions, metric_family, compare_mode, topic)
        route, route_reason = self._resolve_route(
            normalized_question,
            task_mode,
            sql_engine,
            metrics,
            dimensions,
            time_grain,
            time_window,
            compare_mode,
            metric_family,
            topic,
        )

        return SemanticContext(
            question=question,
            normalized_question=normalized_question,
            metrics=metrics,
            dimensions=dimensions,
            time_grain=time_grain,
            time_window=time_window,
            time_window_value=time_window_value,
            compare_mode=compare_mode,
            sort_metric=sort_metric,
            sort_desc=sort_desc,
            limit=limit,
            metric_family=metric_family,
            topic=topic,
            requested_tables=requested_tables,
            relevant_columns=relevant_columns,
            hints=hints,
            notes=notes,
            route=route,
            route_reason=route_reason,
            matched_synonyms=matched_synonyms,
        )

    @staticmethod
    def _merge_detected_values(primary: tuple[str, ...], secondary: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(dict.fromkeys([*primary, *secondary]))

    def _detect_keywords(self, question: str, specs: tuple[KeywordSpec, ...]) -> tuple[str, ...]:
        lowered = question.lower()
        scored: list[tuple[int, str]] = []
        for spec in specs:
            position = self._first_keyword_position(lowered, spec.keywords)
            if position >= 0:
                scored.append((position, spec.name))
        scored.sort(key=lambda item: item[0])
        return tuple(name for _, name in scored)

    @staticmethod
    def _first_keyword_position(lowered: str, keywords: tuple[str, ...]) -> int:
        positions = [lowered.find(keyword.lower()) for keyword in keywords if lowered.find(keyword.lower()) >= 0]
        return min(positions) if positions else -1

    @staticmethod
    def _detect_topic(question: str, metrics: tuple[str, ...]) -> str:
        inventory_metrics = INVENTORY_SNAPSHOT_METRICS | INVENTORY_FLOW_METRICS
        if (
            "库存" in question
            or "仓库" in question
            or "缺货" in question
            or "入库" in question
            or "出库" in question
            or any(metric in metrics for metric in inventory_metrics)
        ):
            return "inventory"
        if any(metric in metrics for metric in ("fulfillment_rate", "avg_fulfillment_hours")) or "履约" in question:
            return "fulfillment"
        if any(metric in metrics for metric in ("repeat_purchase_rate", "repeat_user_count")) or "复购" in question:
            return "repeat_purchase"
        return "sales"

    @staticmethod
    def _normalize_metrics(question: str, metrics: tuple[str, ...], topic: str) -> tuple[str, ...]:
        if topic != "inventory":
            return metrics
        if metrics:
            return metrics
        if any(keyword in question for keyword in ("入库", "出库", "流水")):
            return ("inbound_qty", "outbound_qty")
        return ("inventory_qty",)

    @staticmethod
    def _detect_compare_mode(question: str) -> str | None:
        if "同比" in question:
            return "yoy"
        if "环比" in question:
            return "mom"
        return None

    @staticmethod
    def _detect_time_grain(question: str) -> str | None:
        lowered = question.lower()
        if any(keyword in lowered for keyword in ("按天", "按日", "每日", "每天", "日趋势")):
            return "day"
        if any(keyword in lowered for keyword in ("按月", "每月", "月趋势")):
            return "month"
        return None

    @staticmethod
    def _detect_time_window(question: str) -> tuple[str | None, int | None]:
        match = re.search(r"近\s*(\d+)\s*天", question)
        if match:
            return "rolling_days", int(match.group(1))
        if "昨天" in question:
            return "yesterday", None
        if "今天" in question:
            return "today", None
        if "本月" in question:
            return "this_month", None
        if "上月" in question:
            return "last_month", None
        if "本周" in question:
            return "this_week", None
        return None, None

    @staticmethod
    def _detect_limit(question: str) -> int | None:
        match = re.search(r"(?:top|前)\s*(\d+)", question, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None

    def _detect_sort(self, question: str, metrics: tuple[str, ...]) -> tuple[str | None, bool]:
        lowered = question.lower()
        sort_desc = "升序" not in question
        match = re.search(r"按(.+?)(升序|降序)", question)
        if match:
            target = match.group(1)
            for spec in METRIC_SPECS:
                if any(keyword in target.lower() for keyword in spec.keywords):
                    return spec.name, match.group(2) == "降序"
        if "排名" in question or "top" in lowered or re.search(r"前\s*\d+", question) or "降序" in question or "升序" in question:
            return (metrics[0] if metrics else None), sort_desc
        return None, True

    @staticmethod
    def _detect_metric_family(dimensions: tuple[str, ...], topic: str) -> str:
        if topic == "inventory":
            if any(dimension in PRODUCT_DIMENSIONS for dimension in dimensions):
                return "product"
            if any(dimension in WAREHOUSE_DIMENSIONS for dimension in dimensions):
                return "warehouse"
            return "inventory"
        if any(dimension in PRODUCT_DIMENSIONS for dimension in dimensions):
            return "product"
        return "store"

    def _resolve_tables(
        self,
        question: str,
        metrics: tuple[str, ...],
        dimensions: tuple[str, ...],
        metric_family: str,
        topic: str,
    ) -> tuple[str, ...]:
        tables: list[str] = []
        if topic == "inventory":
            if any(metric in INVENTORY_SNAPSHOT_METRICS for metric in metrics) or any(
                keyword in question for keyword in ("库存", "可用库存", "预占库存", "缺货", "库存金额")
            ):
                tables.append("fct_inventory_snapshot")
            if any(metric in INVENTORY_FLOW_METRICS for metric in metrics) or any(keyword in question for keyword in ("入库", "出库", "流水")):
                tables.append("fct_inventory_flow")
            if any(dimension in WAREHOUSE_DIMENSIONS for dimension in dimensions) or "仓库" in question:
                tables.append("dim_warehouse")
            if not tables:
                tables.append("fct_inventory_snapshot")
            if any(table in tables for table in ("fct_inventory_snapshot", "fct_inventory_flow")):
                tables.append("dim_warehouse")
            return tuple(dict.fromkeys(tables))

        if topic == "fulfillment":
            tables.append("fct_order_main")
            if any(dimension in STORE_DIMENSIONS for dimension in dimensions):
                tables.append("dim_store")
            return tuple(dict.fromkeys(tables))

        if metric_family == "product":
            if any(metric in metrics for metric in ("gmv", "order_count", "net_payment_amount", "refund_rate", "avg_order_amount", "repeat_purchase_rate", "repeat_user_count")):
                tables.extend(("fct_order_item", "fct_order_main"))
            if any(metric in metrics for metric in ("refund_amount", "refund_rate")):
                tables.extend(("fct_refund_item", "fct_refund_main"))
        else:
            if any(metric in metrics for metric in ("gmv", "order_count", "net_payment_amount", "refund_rate", "avg_order_amount", "repeat_purchase_rate", "repeat_user_count")):
                tables.append("fct_order_main")
            if any(metric in metrics for metric in ("refund_amount", "refund_rate")):
                tables.append("fct_refund_main")
            if any(dimension in STORE_DIMENSIONS for dimension in dimensions):
                tables.append("dim_store")
        if not metrics:
            tables.extend(("fct_order_main", "dim_store"))
        return tuple(dict.fromkeys(tables))

    def _resolve_columns(
        self,
        metrics: tuple[str, ...],
        dimensions: tuple[str, ...],
        metric_family: str,
        time_grain: str | None,
        topic: str,
    ) -> dict[str, tuple[str, ...]]:
        columns: dict[str, list[str]] = {}

        if topic == "inventory":
            if any(metric in INVENTORY_SNAPSHOT_METRICS for metric in metrics):
                columns["fct_inventory_snapshot"] = [
                    "snapshot_date",
                    "warehouse_id",
                    "product_id",
                    "brand_name",
                    "series_name",
                    "category_name",
                    "product_name",
                    "inventory_qty",
                    "available_qty",
                    "reserved_qty",
                    "stock_status",
                    "inventory_amount",
                ]
            if any(metric in INVENTORY_FLOW_METRICS for metric in metrics):
                columns["fct_inventory_flow"] = [
                    "flow_date",
                    "warehouse_id",
                    "product_id",
                    "brand_name",
                    "series_name",
                    "category_name",
                    "product_name",
                    "direction",
                    "quantity",
                ]
            warehouse_dimensions = [dimension for dimension in dimensions if dimension in WAREHOUSE_DIMENSIONS]
            if warehouse_dimensions:
                columns["dim_warehouse"] = ["warehouse_id", *warehouse_dimensions]
            elif "fct_inventory_snapshot" in columns or "fct_inventory_flow" in columns:
                columns["dim_warehouse"] = ["warehouse_id", "warehouse_name"]
            if time_grain in {"day", "month"}:
                if "fct_inventory_snapshot" in columns and "snapshot_date" not in columns["fct_inventory_snapshot"]:
                    columns["fct_inventory_snapshot"].append("snapshot_date")
                if "fct_inventory_flow" in columns and "flow_date" not in columns["fct_inventory_flow"]:
                    columns["fct_inventory_flow"].append("flow_date")
            return {table: tuple(dict.fromkeys(items)) for table, items in columns.items()}

        if topic == "fulfillment":
            columns["fct_order_main"] = ["order_id", "store_id", "order_date", "pay_status", "pay_time", "finish_time", "order_status"]
            if any(dimension in STORE_DIMENSIONS for dimension in dimensions):
                columns["dim_store"] = ["store_id", *dimensions]
            return {table: tuple(dict.fromkeys(items)) for table, items in columns.items()}

        if metric_family == "product":
            if any(metric in metrics for metric in ("gmv", "order_count", "net_payment_amount", "avg_order_amount", "repeat_purchase_rate", "repeat_user_count", "refund_rate")):
                columns["fct_order_item"] = ["order_id", "pay_amount", "refunded_amount", "brand_name", "series_name", "category_name", "product_name"]
                columns["fct_order_main"] = ["order_id", "user_id", "order_date", "pay_status", "payment_amount", "net_payment_amount"]
            if any(metric in metrics for metric in ("refund_amount", "refund_rate")):
                columns["fct_refund_item"] = ["refund_id", "order_id", "refund_amount", "brand_name", "series_name", "category_name", "product_name"]
                columns["fct_refund_main"] = ["refund_id", "order_id", "refund_date", "refund_status", "refund_amount"]
        else:
            if any(metric in metrics for metric in ("gmv", "order_count", "net_payment_amount", "avg_order_amount", "repeat_purchase_rate", "repeat_user_count", "refund_rate")):
                columns["fct_order_main"] = ["order_id", "user_id", "store_id", "order_date", "pay_status", "payment_amount", "net_payment_amount"]
            if any(metric in metrics for metric in ("refund_amount", "refund_rate")):
                columns["fct_refund_main"] = ["refund_id", "order_id", "store_id", "refund_date", "refund_status", "refund_amount"]
            if any(dimension in STORE_DIMENSIONS for dimension in dimensions):
                columns["dim_store"] = ["store_id", *dimensions]

        if not metrics and "fct_order_main" not in columns:
            columns["fct_order_main"] = ["order_id", "store_id", "order_date", "pay_status", "payment_amount"]
        if time_grain in {"day", "month"}:
            if "fct_order_main" in columns and "order_date" not in columns["fct_order_main"]:
                columns["fct_order_main"].append("order_date")
            if "fct_refund_main" in columns and "refund_date" not in columns["fct_refund_main"]:
                columns["fct_refund_main"].append("refund_date")
        return {table: tuple(dict.fromkeys(items)) for table, items in columns.items()}

    @staticmethod
    def _build_hints(
        metrics: tuple[str, ...],
        dimensions: tuple[str, ...],
        time_grain: str | None,
        time_window: str | None,
        metric_family: str,
        compare_mode: str | None,
        topic: str,
    ) -> tuple[str, ...]:
        hints: list[str] = []
        if topic == "inventory":
            if any(metric in metrics for metric in INVENTORY_SNAPSHOT_METRICS):
                hints.append("库存类指标默认基于库存快照表，未指定日期时默认取最新快照日。")
            if any(metric in metrics for metric in INVENTORY_FLOW_METRICS):
                hints.append("入库量和出库量默认基于库存流水表，分别按 direction='入库'、'出库' 汇总。")
            if "stockout_rate" in metrics:
                hints.append("缺货率口径为缺货SKU数/总SKU数，缺货定义为 available_qty<=0 或 stock_status='缺货'。")
            if dimensions:
                hints.append("仓库、大区、省市等库存维度字段来自 dim_warehouse；系列/类目/商品字段来自库存事实表。")
        else:
            if "gmv" in metrics or "avg_order_amount" in metrics:
                hints.append("GMV 默认取已支付订单金额。")
            if "refund_amount" in metrics or "refund_rate" in metrics:
                hints.append("退款相关分析默认限定退款成功。")
            if dimensions:
                hints.append("最终结果粒度必须严格等于用户要求的维度。")
            if topic == "repeat_purchase":
                hints.append("复购口径为时间窗内同一维度下用户支付订单数>=2。")
            if topic == "fulfillment":
                hints.append("履约率口径为已完成订单数/已支付订单数。")
            if metric_family == "product":
                hints.append("商品维度分析优先使用订单明细/退款明细金额，不要回退到订单主表整单金额。")

        if compare_mode == "mom":
            hints.append("环比默认与上一周期同口径比较。")
        if compare_mode == "yoy":
            hints.append("同比默认与去年同期同口径比较。")
        if time_grain == "day":
            hints.append("需要按天输出时，日期字段统一命名为 stat_date。")
        if time_grain == "month":
            hints.append("需要按月输出时，月份字段统一命名为 stat_month。")
        if time_window == "rolling_days":
            hints.append("滚动天数区间使用当前日期回溯。")
        return tuple(hints)

    @staticmethod
    def _build_notes(
        question: str,
        metrics: tuple[str, ...],
        dimensions: tuple[str, ...],
        metric_family: str,
        compare_mode: str | None,
        topic: str,
    ) -> tuple[str, ...]:
        notes: list[str] = []
        if "蒙牛" in question:
            notes.append("当前演示数据全部为蒙牛品牌，不需要额外品牌过滤。")
        if topic == "inventory":
            notes.append("库存量、可用库存、预占库存、库存金额来自 fct_inventory_snapshot。")
            notes.append("入库量、出库量来自 fct_inventory_flow。")
            if any(dimension in WAREHOUSE_DIMENSIONS for dimension in dimensions):
                notes.append("仓库、大区、省市等维度字段来自 dim_warehouse。")
        else:
            if metric_family == "store" and any(dimension in STORE_DIMENSIONS for dimension in dimensions):
                notes.append("渠道、省市、门店等维度字段来自 dim_store。")
            if "refund_rate" in metrics:
                notes.append("退款率口径为成功退款订单数/已支付订单数。")
        if compare_mode:
            notes.append("对比类题目会输出当前值、对比值和变化率。")
        return tuple(notes)

    def _resolve_route(
        self,
        question: str,
        task_mode: str,
        sql_engine: str,
        metrics: tuple[str, ...],
        dimensions: tuple[str, ...],
        time_grain: str | None,
        time_window: str | None,
        compare_mode: str | None,
        metric_family: str,
        topic: str,
    ) -> tuple[str, str]:
        if task_mode != "dql":
            return "llm", "非查询任务仍由大模型生成。"
        if sql_engine != "mysql":
            return "llm", "查询执行和校验当前固定走 MySQL。"
        if not metrics:
            return "llm", "未识别到稳定指标，交给大模型兜底。"
        if any(keyword in question for keyword in UNSAFE_TEMPLATE_HINTS):
            return "llm", "检测到复杂分析关键词，交给大模型兜底。"

        if topic == "inventory":
            if set(metrics) - SUPPORTED_INVENTORY_TEMPLATE_METRICS:
                return "llm", "存在库存模板未覆盖的指标。"
            if compare_mode:
                return "llm", "当前库存模板暂不处理同比环比。"
            if set(dimensions) - INVENTORY_DIMENSIONS:
                return "llm", "库存分析出现了模板未覆盖的维度组合。"
            if time_grain not in {None, "day", "month"}:
                return "llm", "库存模板当前只支持汇总、按天和按月。"
            return "template", "命中本地库存分析模板，可直接生成高确定性 SQL。"

        if set(metrics) - SUPPORTED_TEMPLATE_METRICS:
            return "llm", "存在模板未覆盖的指标。"
        dimension_set = set(dimensions)
        if dimension_set and not (
            dimension_set.issubset(STORE_DIMENSIONS) or dimension_set.issubset(PRODUCT_DIMENSIONS)
        ):
            return "llm", "维度来源混合或不稳定，交给大模型兜底。"
        if time_grain not in {None, "day", "month"}:
            return "llm", "时间粒度超出本地模板覆盖范围。"
        if metric_family == "product" and not dimension_set and topic not in {"sales"}:
            return "llm", "缺少商品维度时不走商品专题模板。"
        if topic == "fulfillment" and metric_family == "product":
            return "llm", "当前履约模板只支持门店域维度。"
        if compare_mode:
            if topic in {"repeat_purchase", "fulfillment"}:
                return "llm", "当前同比环比模板只稳定支持销售类核心指标。"
            if len(metrics) != 1:
                return "llm", "同比环比模板当前只稳定支持单一核心指标。"
            if metrics[0] not in {"gmv", "order_count", "refund_amount", "avg_order_amount", "net_payment_amount"}:
                return "llm", "当前同比环比模板只覆盖 GMV、订单量、退款金额、客单价、净销售额。"
            if time_grain is not None:
                return "llm", "同比环比模板当前不处理趋势展开题，交给大模型兜底。"
            if time_window is None:
                return "llm", "同比环比需要明确时间窗口，交给大模型兜底。"
        if topic == "repeat_purchase" and time_grain is not None:
            return "llm", "复购模板当前按汇总题更稳定，趋势题交给大模型兜底。"
        return "template", "命中本地电商分析模板，可直接生成高确定性 SQL。"
