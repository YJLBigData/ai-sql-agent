from __future__ import annotations

from dataclasses import dataclass

from .models import SemanticContext


STORE_DIMENSION_SQL = {
    "channel_name": ("ds.channel_name", "channel_name"),
    "store_name": ("ds.store_name", "store_name"),
    "region_name": ("ds.region_name", "region_name"),
    "province_name": ("ds.province_name", "province_name"),
    "city_name": ("ds.city_name", "city_name"),
}

WAREHOUSE_DIMENSION_SQL = {
    "warehouse_name": ("dw.warehouse_name", "warehouse_name"),
    "warehouse_type": ("dw.warehouse_type", "warehouse_type"),
    "region_name": ("dw.region_name", "region_name"),
    "province_name": ("dw.province_name", "province_name"),
    "city_name": ("dw.city_name", "city_name"),
}

PRODUCT_DIMENSION_SQL = {
    "brand_name": ("{alias}.brand_name", "brand_name"),
    "series_name": ("{alias}.series_name", "series_name"),
    "category_name": ("{alias}.category_name", "category_name"),
    "product_name": ("{alias}.product_name", "product_name"),
}

INVENTORY_SNAPSHOT_METRICS = {"inventory_qty", "available_qty", "reserved_qty", "inventory_amount", "stockout_rate"}
INVENTORY_FLOW_METRICS = {"inbound_qty", "outbound_qty"}


@dataclass(frozen=True)
class PlannerResult:
    sql: str
    route: str
    reason: str


class DeterministicSQLPlanner:
    def plan(self, context: SemanticContext, task_mode: str, sql_engine: str) -> PlannerResult | None:
        if context.route != "template" or task_mode != "dql" or sql_engine != "mysql":
            return None

        if context.topic == "inventory":
            sql = self._plan_inventory_query(context)
        elif context.compare_mode:
            sql = self._plan_comparison_query(context)
        elif context.topic == "repeat_purchase":
            sql = self._plan_repeat_query(context)
        elif context.topic == "fulfillment":
            sql = self._plan_fulfillment_query(context)
        else:
            sql = self._plan_sales_query(context)

        if not sql:
            return None
        return PlannerResult(sql=sql, route="template", reason=context.route_reason)

    def _plan_sales_query(self, context: SemanticContext) -> str:
        family = context.metric_family
        amount_expr = "om.payment_amount" if family == "store" else "oi.pay_amount"
        net_amount_expr = "om.net_payment_amount" if family == "store" else "(oi.pay_amount - oi.refunded_amount)"
        order_id_expr = "om.order_id"
        refund_amount_expr = "rm.refund_amount" if family == "store" else "ri.refund_amount"
        refund_order_id_expr = "rm.order_id" if family == "store" else "ri.order_id"

        order_dim_select = self._dimension_selects(context, family, side="order")
        refund_dim_select = self._dimension_selects(context, family, side="refund")
        order_group_by = self._group_by_columns(context, family, side="order")
        refund_group_by = self._group_by_columns(context, family, side="refund")
        key_columns = self._key_columns(context)

        needs_order = any(metric in context.metrics for metric in ("gmv", "order_count", "net_payment_amount", "avg_order_amount", "refund_rate"))
        needs_refund = any(metric in context.metrics for metric in ("refund_amount", "refund_rate"))
        ctes: list[str] = []

        if needs_order:
            measures = [
                f"SUM({amount_expr}) AS gmv",
                f"COUNT(DISTINCT {order_id_expr}) AS order_count",
            ]
            if "net_payment_amount" in context.metrics:
                measures.append(f"SUM({net_amount_expr}) AS net_payment_amount")
            if "refund_rate" in context.metrics:
                measures.append(f"COUNT(DISTINCT {order_id_expr}) AS paid_order_cnt")
            ctes.append(
                self._build_cte(
                    "order_agg",
                    order_dim_select,
                    measures,
                    self._order_from_clause(family),
                    ["om.pay_status = '已支付'", self._current_time_condition(self._order_date_column(family), context)],
                    order_group_by,
                )
            )

        if needs_refund:
            measures = [f"SUM({refund_amount_expr}) AS refund_amount"]
            if "refund_rate" in context.metrics:
                measures.append(f"COUNT(DISTINCT {refund_order_id_expr}) AS refund_order_cnt")
            ctes.append(
                self._build_cte(
                    "refund_agg",
                    refund_dim_select,
                    measures,
                    self._refund_from_clause(family),
                    ["rm.refund_status = '退款成功'", self._current_time_condition(self._refund_date_column(family), context)],
                    refund_group_by,
                )
            )

        if needs_order and needs_refund:
            ctes.append(self._build_key_cte(key_columns))
            return self._final_join_sql(context, ctes, key_columns)
        if needs_order:
            return self._single_cte_sql(context, ctes, key_columns, metric_source="oa")
        if needs_refund:
            return self._single_cte_sql(context, ctes, key_columns, metric_source="ra")
        return ""

    def _plan_repeat_query(self, context: SemanticContext) -> str:
        family = context.metric_family
        dim_select = self._dimension_selects(context, family, side="order")
        group_columns = self._group_by_columns(context, family, side="order")
        user_group_columns = [*group_columns, "om.user_id"]
        key_columns = self._key_columns(context)

        user_order_cte = self._build_cte(
            "user_order_agg",
            [*dim_select, "om.user_id AS user_id"],
            ["COUNT(DISTINCT om.order_id) AS order_count"],
            self._order_from_clause(family),
            ["om.pay_status = '已支付'", self._current_time_condition(self._order_date_column(family), context)],
            user_group_columns,
        )
        repeat_cte = self._build_cte(
            "repeat_agg",
            self._final_dimension_selects(key_columns, source_alias="uoa"),
            [
                "COUNT(DISTINCT uoa.user_id) AS paid_user_cnt",
                "SUM(CASE WHEN uoa.order_count >= 2 THEN 1 ELSE 0 END) AS repeat_user_count",
            ],
            "    FROM user_order_agg uoa",
            [],
            [f"uoa.{column}" for column in key_columns],
        )
        select_items = self._final_dimension_selects(key_columns, source_alias="ra")
        if "repeat_user_count" in context.metrics:
            select_items.append("COALESCE(ra.repeat_user_count, 0) AS repeat_user_count")
        if "repeat_purchase_rate" in context.metrics:
            select_items.append(
                "ROUND(COALESCE(ra.repeat_user_count, 0) / NULLIF(COALESCE(ra.paid_user_cnt, 0), 0), 4) AS repeat_purchase_rate"
            )
        return (
            "WITH\n"
            f"{user_order_cte},\n"
            f"{repeat_cte}\n"
            "SELECT\n    " + ",\n    ".join(select_items) + "\n"
            "FROM repeat_agg ra\n"
            f"{self._build_order_by(context)}{self._build_limit(context)};"
        )

    def _plan_fulfillment_query(self, context: SemanticContext) -> str:
        key_columns = self._key_columns(context)
        dim_select = self._dimension_selects(context, "store", side="order")
        group_columns = self._group_by_columns(context, "store", side="order")
        cte = self._build_cte(
            "fulfillment_agg",
            dim_select,
            [
                "COUNT(DISTINCT om.order_id) AS paid_order_cnt",
                "SUM(CASE WHEN om.finish_time IS NOT NULL OR om.order_status = '已完成' THEN 1 ELSE 0 END) AS fulfilled_order_cnt",
                "AVG(CASE WHEN om.pay_time IS NOT NULL AND om.finish_time IS NOT NULL THEN TIMESTAMPDIFF(HOUR, om.pay_time, om.finish_time) END) AS avg_fulfillment_hours",
            ],
            self._order_from_clause("store"),
            ["om.pay_status = '已支付'", self._current_time_condition("om.order_date", context)],
            group_columns,
        )
        select_items = self._final_dimension_selects(key_columns, source_alias="fa")
        if "fulfillment_rate" in context.metrics:
            select_items.append(
                "ROUND(COALESCE(fa.fulfilled_order_cnt, 0) / NULLIF(COALESCE(fa.paid_order_cnt, 0), 0), 4) AS fulfillment_rate"
            )
        if "avg_fulfillment_hours" in context.metrics:
            select_items.append("ROUND(COALESCE(fa.avg_fulfillment_hours, 0), 2) AS avg_fulfillment_hours")
        return (
            "WITH\n"
            f"{cte}\n"
            "SELECT\n    " + ",\n    ".join(select_items) + "\n"
            "FROM fulfillment_agg fa\n"
            f"{self._build_order_by(context)}{self._build_limit(context)};"
        )

    def _plan_inventory_query(self, context: SemanticContext) -> str:
        key_columns = self._key_columns(context)
        ctes: list[str] = []
        needs_snapshot = any(metric in INVENTORY_SNAPSHOT_METRICS for metric in context.metrics)
        needs_flow = any(metric in INVENTORY_FLOW_METRICS for metric in context.metrics)

        if needs_snapshot:
            snapshot_dim_select = self._inventory_dimension_selects(context, side="snapshot")
            snapshot_group_by = self._inventory_group_by_columns(context, side="snapshot")
            snapshot_conditions = [self._inventory_snapshot_condition(context)]
            snapshot_measures: list[str] = []
            if "inventory_qty" in context.metrics:
                snapshot_measures.append("SUM(inv.inventory_qty) AS inventory_qty")
            if "available_qty" in context.metrics:
                snapshot_measures.append("SUM(inv.available_qty) AS available_qty")
            if "reserved_qty" in context.metrics:
                snapshot_measures.append("SUM(inv.reserved_qty) AS reserved_qty")
            if "inventory_amount" in context.metrics:
                snapshot_measures.append("SUM(inv.inventory_amount) AS inventory_amount")
            if "stockout_rate" in context.metrics:
                snapshot_measures.extend(
                    [
                        "SUM(CASE WHEN inv.available_qty <= 0 OR inv.stock_status = '缺货' THEN 1 ELSE 0 END) AS stockout_sku_cnt",
                        "COUNT(DISTINCT inv.product_id) AS total_sku_cnt",
                    ]
                )
            ctes.append(
                self._build_cte(
                    "snapshot_agg",
                    snapshot_dim_select,
                    snapshot_measures,
                    self._inventory_snapshot_from_clause(),
                    snapshot_conditions,
                    snapshot_group_by,
                )
            )

        if needs_flow:
            flow_dim_select = self._inventory_dimension_selects(context, side="flow")
            flow_group_by = self._inventory_group_by_columns(context, side="flow")
            flow_measures: list[str] = []
            if "inbound_qty" in context.metrics:
                flow_measures.append("SUM(CASE WHEN flow.direction = '入库' THEN flow.quantity ELSE 0 END) AS inbound_qty")
            if "outbound_qty" in context.metrics:
                flow_measures.append("SUM(CASE WHEN flow.direction = '出库' THEN flow.quantity ELSE 0 END) AS outbound_qty")
            ctes.append(
                self._build_cte(
                    "flow_agg",
                    flow_dim_select,
                    flow_measures,
                    self._inventory_flow_from_clause(),
                    [self._inventory_flow_condition(context)],
                    flow_group_by,
                )
            )

        if needs_snapshot and needs_flow:
            ctes.append(self._build_key_cte(key_columns, left_name="snapshot_agg", right_name="flow_agg"))
            select_items = self._final_dimension_selects(key_columns, source_alias="ak")
            select_items.extend(self._inventory_metric_selects(context))
            return (
                "WITH\n"
                + ",\n".join(ctes)
                + "\nSELECT\n    "
                + ",\n    ".join(select_items)
                + "\nFROM all_keys ak\n"
                + f"LEFT JOIN snapshot_agg sa\n  ON {self._join_condition('ak', 'sa', key_columns)}\n"
                + f"LEFT JOIN flow_agg fa\n  ON {self._join_condition('ak', 'fa', key_columns)}\n"
                + f"{self._build_order_by(context)}{self._build_limit(context)};"
            )

        if needs_snapshot:
            select_items = self._final_dimension_selects(key_columns, source_alias="sa")
            select_items.extend(self._inventory_metric_selects(context))
            return (
                "WITH\n"
                + ",\n".join(ctes)
                + "\nSELECT\n    "
                + ",\n    ".join(select_items)
                + "\nFROM snapshot_agg sa\n"
                + f"{self._build_order_by(context)}{self._build_limit(context)};"
            )

        if needs_flow:
            select_items = self._final_dimension_selects(key_columns, source_alias="fa")
            select_items.extend(self._inventory_metric_selects(context))
            return (
                "WITH\n"
                + ",\n".join(ctes)
                + "\nSELECT\n    "
                + ",\n    ".join(select_items)
                + "\nFROM flow_agg fa\n"
                + f"{self._build_order_by(context)}{self._build_limit(context)};"
            )
        return ""

    def _plan_comparison_query(self, context: SemanticContext) -> str:
        metric = context.metrics[0]
        family = context.metric_family
        key_columns = self._key_columns(context)
        dim_select_current = self._dimension_selects(context, family, side="order" if metric != "refund_amount" else "refund")
        dim_select_previous = list(dim_select_current)
        group_columns = self._group_by_columns(context, family, side="order" if metric != "refund_amount" else "refund")
        previous_metric_alias = f"previous_{metric}"
        current_metric_alias = f"current_{metric}"
        rate_alias = "yoy_rate" if context.compare_mode == "yoy" else "mom_rate"

        if metric == "refund_amount":
            current_cte = self._build_cte(
                "current_agg",
                dim_select_current,
                ["SUM(" + ("rm.refund_amount" if family == "store" else "ri.refund_amount") + f") AS {current_metric_alias}"],
                self._refund_from_clause(family),
                ["rm.refund_status = '退款成功'", self._current_time_condition(self._refund_date_column(family), context)],
                group_columns,
            )
            previous_cte = self._build_cte(
                "previous_agg",
                dim_select_previous,
                ["SUM(" + ("rm.refund_amount" if family == "store" else "ri.refund_amount") + f") AS {previous_metric_alias}"],
                self._refund_from_clause(family),
                ["rm.refund_status = '退款成功'", self._previous_time_condition(self._refund_date_column(family), context)],
                group_columns,
            )
        elif metric == "avg_order_amount":
            current_cte = self._build_cte(
                "current_agg",
                dim_select_current,
                [
                    "SUM(" + ("om.payment_amount" if family == "store" else "oi.pay_amount") + ") AS gmv",
                    "COUNT(DISTINCT om.order_id) AS order_count",
                ],
                self._order_from_clause(family),
                ["om.pay_status = '已支付'", self._current_time_condition(self._order_date_column(family), context)],
                group_columns,
            )
            previous_cte = self._build_cte(
                "previous_agg",
                dim_select_previous,
                [
                    "SUM(" + ("om.payment_amount" if family == "store" else "oi.pay_amount") + ") AS gmv",
                    "COUNT(DISTINCT om.order_id) AS order_count",
                ],
                self._order_from_clause(family),
                ["om.pay_status = '已支付'", self._previous_time_condition(self._order_date_column(family), context)],
                group_columns,
            )
        else:
            metric_expr = {
                "gmv": "SUM(" + ("om.payment_amount" if family == "store" else "oi.pay_amount") + ")",
                "order_count": "COUNT(DISTINCT om.order_id)",
                "net_payment_amount": "SUM(" + ("om.net_payment_amount" if family == "store" else "(oi.pay_amount - oi.refunded_amount)") + ")",
            }[metric]
            current_cte = self._build_cte(
                "current_agg",
                dim_select_current,
                [f"{metric_expr} AS {current_metric_alias}"],
                self._order_from_clause(family),
                ["om.pay_status = '已支付'", self._current_time_condition(self._order_date_column(family), context)],
                group_columns,
            )
            previous_cte = self._build_cte(
                "previous_agg",
                dim_select_previous,
                [f"{metric_expr} AS {previous_metric_alias}"],
                self._order_from_clause(family),
                ["om.pay_status = '已支付'", self._previous_time_condition(self._order_date_column(family), context)],
                group_columns,
            )

        key_cte = self._build_key_cte(key_columns, left_name="current_agg", right_name="previous_agg")
        select_items = self._final_dimension_selects(key_columns, source_alias="ak")
        if metric == "avg_order_amount":
            select_items.extend(
                [
                    "ROUND(COALESCE(ca.gmv, 0) / NULLIF(COALESCE(ca.order_count, 0), 0), 2) AS current_avg_order_amount",
                    "ROUND(COALESCE(pa.gmv, 0) / NULLIF(COALESCE(pa.order_count, 0), 0), 2) AS previous_avg_order_amount",
                    "ROUND((COALESCE(ca.gmv, 0) / NULLIF(COALESCE(ca.order_count, 0), 0) - COALESCE(pa.gmv, 0) / NULLIF(COALESCE(pa.order_count, 0), 0)) / NULLIF(COALESCE(pa.gmv, 0) / NULLIF(COALESCE(pa.order_count, 0), 0), 0), 4) AS "
                    + rate_alias,
                ]
            )
        else:
            select_items.extend(
                [
                    f"COALESCE(ca.{current_metric_alias}, 0) AS {current_metric_alias}",
                    f"COALESCE(pa.{previous_metric_alias}, 0) AS {previous_metric_alias}",
                    f"ROUND((COALESCE(ca.{current_metric_alias}, 0) - COALESCE(pa.{previous_metric_alias}, 0)) / NULLIF(COALESCE(pa.{previous_metric_alias}, 0), 0), 4) AS {rate_alias}",
                ]
            )
        return (
            "WITH\n"
            f"{current_cte},\n"
            f"{previous_cte},\n"
            f"{key_cte}\n"
            "SELECT\n    " + ",\n    ".join(select_items) + "\n"
            "FROM all_keys ak\n"
            f"LEFT JOIN current_agg ca\n  ON {self._join_condition('ak', 'ca', key_columns)}\n"
            f"LEFT JOIN previous_agg pa\n  ON {self._join_condition('ak', 'pa', key_columns)}\n"
            f"{self._build_order_by(context)}{self._build_limit(context)};"
        )

    def _final_join_sql(self, context: SemanticContext, ctes: list[str], key_columns: list[str]) -> str:
        select_items = self._final_dimension_selects(key_columns, source_alias="ak")
        select_items.extend(self._final_metric_selects(context, order_alias="oa", refund_alias="ra"))
        return (
            "WITH\n"
            + ",\n".join(ctes)
            + "\nSELECT\n    "
            + ",\n    ".join(select_items)
            + "\nFROM all_keys ak\n"
            + f"LEFT JOIN order_agg oa\n  ON {self._join_condition('ak', 'oa', key_columns)}\n"
            + f"LEFT JOIN refund_agg ra\n  ON {self._join_condition('ak', 'ra', key_columns)}\n"
            + f"{self._build_order_by(context)}{self._build_limit(context)};"
        )

    def _single_cte_sql(self, context: SemanticContext, ctes: list[str], key_columns: list[str], metric_source: str) -> str:
        source_alias = metric_source
        select_items = self._final_dimension_selects(key_columns, source_alias=source_alias)
        select_items.extend(self._final_metric_selects(context, order_alias="oa", refund_alias="ra"))
        from_line = "FROM order_agg oa" if metric_source == "oa" else "FROM refund_agg ra"
        return (
            "WITH\n"
            + ",\n".join(ctes)
            + "\nSELECT\n    "
            + ",\n    ".join(select_items)
            + f"\n{from_line}\n"
            + f"{self._build_order_by(context)}{self._build_limit(context)};"
        )

    def _final_metric_selects(self, context: SemanticContext, order_alias: str, refund_alias: str) -> list[str]:
        items: list[str] = []
        for metric in context.metrics:
            if metric == "gmv":
                items.append(f"COALESCE({order_alias}.gmv, 0) AS gmv")
            elif metric == "order_count":
                items.append(f"COALESCE({order_alias}.order_count, 0) AS order_count")
            elif metric == "net_payment_amount":
                items.append(f"COALESCE({order_alias}.net_payment_amount, 0) AS net_payment_amount")
            elif metric == "avg_order_amount":
                items.append(
                    f"ROUND(COALESCE({order_alias}.gmv, 0) / NULLIF(COALESCE({order_alias}.order_count, 0), 0), 2) AS avg_order_amount"
                )
            elif metric == "refund_amount":
                items.append(f"COALESCE({refund_alias}.refund_amount, 0) AS refund_amount")
            elif metric == "refund_rate":
                items.append(
                    f"ROUND(COALESCE({refund_alias}.refund_order_cnt, 0) / NULLIF(COALESCE({order_alias}.paid_order_cnt, 0), 0), 4) AS refund_rate"
                )
        return items

    def _inventory_metric_selects(self, context: SemanticContext) -> list[str]:
        items: list[str] = []
        for metric in context.metrics:
            if metric == "inventory_qty":
                items.append("COALESCE(sa.inventory_qty, 0) AS inventory_qty")
            elif metric == "available_qty":
                items.append("COALESCE(sa.available_qty, 0) AS available_qty")
            elif metric == "reserved_qty":
                items.append("COALESCE(sa.reserved_qty, 0) AS reserved_qty")
            elif metric == "inventory_amount":
                items.append("COALESCE(sa.inventory_amount, 0) AS inventory_amount")
            elif metric == "stockout_rate":
                items.append("ROUND(COALESCE(sa.stockout_sku_cnt, 0) / NULLIF(COALESCE(sa.total_sku_cnt, 0), 0), 4) AS stockout_rate")
            elif metric == "inbound_qty":
                items.append("COALESCE(fa.inbound_qty, 0) AS inbound_qty")
            elif metric == "outbound_qty":
                items.append("COALESCE(fa.outbound_qty, 0) AS outbound_qty")
        return items

    def _dimension_selects(self, context: SemanticContext, family: str, side: str) -> list[str]:
        select_items: list[str] = []
        mapping = STORE_DIMENSION_SQL if family == "store" else PRODUCT_DIMENSION_SQL
        alias = "ds" if family == "store" else ("oi" if side == "order" else "ri")
        for dimension in context.dimensions:
            expression, column_alias = mapping[dimension]
            select_expr = expression if family == "store" else expression.format(alias=alias)
            select_items.append(f"{select_expr} AS {column_alias}")
        time_item = self._time_dimension_select(context, side)
        if time_item:
            select_items.append(time_item)
        return select_items

    def _inventory_dimension_selects(self, context: SemanticContext, side: str) -> list[str]:
        select_items: list[str] = []
        fact_alias = "inv" if side == "snapshot" else "flow"
        for dimension in context.dimensions:
            if dimension in WAREHOUSE_DIMENSION_SQL:
                expression, column_alias = WAREHOUSE_DIMENSION_SQL[dimension]
                select_items.append(f"{expression} AS {column_alias}")
            elif dimension in PRODUCT_DIMENSION_SQL:
                expression, column_alias = PRODUCT_DIMENSION_SQL[dimension]
                select_items.append(f"{expression.format(alias=fact_alias)} AS {column_alias}")
        time_item = self._inventory_time_dimension_select(context, side)
        if time_item:
            select_items.append(time_item)
        return select_items

    def _group_by_columns(self, context: SemanticContext, family: str, side: str) -> list[str]:
        mapping = STORE_DIMENSION_SQL if family == "store" else PRODUCT_DIMENSION_SQL
        alias = "ds" if family == "store" else ("oi" if side == "order" else "ri")
        group_items: list[str] = []
        for dimension in context.dimensions:
            expression, _ = mapping[dimension]
            group_items.append(expression if family == "store" else expression.format(alias=alias))
        time_group = self._time_dimension_group_by(context, side)
        if time_group:
            group_items.append(time_group)
        return group_items

    def _inventory_group_by_columns(self, context: SemanticContext, side: str) -> list[str]:
        group_items: list[str] = []
        fact_alias = "inv" if side == "snapshot" else "flow"
        for dimension in context.dimensions:
            if dimension in WAREHOUSE_DIMENSION_SQL:
                expression, _ = WAREHOUSE_DIMENSION_SQL[dimension]
                group_items.append(expression)
            elif dimension in PRODUCT_DIMENSION_SQL:
                expression, _ = PRODUCT_DIMENSION_SQL[dimension]
                group_items.append(expression.format(alias=fact_alias))
        time_group = self._inventory_time_dimension_group_by(context, side)
        if time_group:
            group_items.append(time_group)
        return group_items

    def _key_columns(self, context: SemanticContext) -> list[str]:
        keys = list(context.dimensions)
        if context.time_grain == "day":
            keys.append("stat_date")
        elif context.time_grain == "month":
            keys.append("stat_month")
        return keys

    def _build_cte(
        self,
        name: str,
        dimensions: list[str],
        measures: list[str],
        from_clause: str,
        conditions: list[str],
        group_by: list[str],
    ) -> str:
        select_lines = [*dimensions, *measures]
        rendered_conditions = [condition for condition in conditions if condition]
        where_block = ""
        if rendered_conditions:
            where_block = "WHERE " + "\n      AND ".join(rendered_conditions)
        group_block = ""
        if group_by:
            group_block = "\n    GROUP BY " + ", ".join(group_by)
        return (
            f"{name} AS (\n"
            f"    SELECT\n        " + ",\n        ".join(select_lines) + "\n"
            f"{from_clause}\n"
            f"    {where_block}{group_block}\n"
            ")"
        )

    def _build_key_cte(self, key_columns: list[str], left_name: str = "order_agg", right_name: str = "refund_agg") -> str:
        if not key_columns:
            return (
                "all_keys AS (\n"
                "    SELECT 1 AS join_key\n"
                f"    FROM {left_name}\n"
                "    UNION\n"
                "    SELECT 1 AS join_key\n"
                f"    FROM {right_name}\n"
                ")"
            )
        key_list = ", ".join(key_columns)
        return (
            "all_keys AS (\n"
            f"    SELECT DISTINCT {key_list}\n"
            f"    FROM {left_name}\n"
            "    UNION\n"
            f"    SELECT DISTINCT {key_list}\n"
            f"    FROM {right_name}\n"
            ")"
        )

    @staticmethod
    def _final_dimension_selects(key_columns: list[str], source_alias: str) -> list[str]:
        return [f"{source_alias}.{column} AS {column}" for column in key_columns]

    @staticmethod
    def _join_condition(left_alias: str, right_alias: str, key_columns: list[str]) -> str:
        if not key_columns:
            return "1 = 1"
        return " AND ".join(f"{left_alias}.{column} = {right_alias}.{column}" for column in key_columns)

    @staticmethod
    def _order_from_clause(family: str) -> str:
        if family == "product":
            return (
                "    FROM fct_order_item oi\n"
                "    JOIN fct_order_main om\n"
                "      ON oi.order_id = om.order_id"
            )
        return (
            "    FROM fct_order_main om\n"
            "    JOIN dim_store ds\n"
            "      ON om.store_id = ds.store_id"
        )

    @staticmethod
    def _refund_from_clause(family: str) -> str:
        if family == "product":
            return (
                "    FROM fct_refund_item ri\n"
                "    JOIN fct_refund_main rm\n"
                "      ON ri.refund_id = rm.refund_id"
            )
        return (
            "    FROM fct_refund_main rm\n"
            "    JOIN dim_store ds\n"
            "      ON rm.store_id = ds.store_id"
        )

    @staticmethod
    def _inventory_snapshot_from_clause() -> str:
        return (
            "    FROM fct_inventory_snapshot inv\n"
            "    JOIN dim_warehouse dw\n"
            "      ON inv.warehouse_id = dw.warehouse_id"
        )

    @staticmethod
    def _inventory_flow_from_clause() -> str:
        return (
            "    FROM fct_inventory_flow flow\n"
            "    JOIN dim_warehouse dw\n"
            "      ON flow.warehouse_id = dw.warehouse_id"
        )

    @staticmethod
    def _order_date_column(family: str) -> str:
        return "om.order_date"

    @staticmethod
    def _refund_date_column(family: str) -> str:
        return "rm.refund_date"

    @staticmethod
    def _time_dimension_select(context: SemanticContext, side: str) -> str | None:
        if context.time_grain == "day":
            source_column = "om.order_date" if side == "order" else "rm.refund_date"
            return f"{source_column} AS stat_date"
        if context.time_grain == "month":
            source_column = "om.order_date" if side == "order" else "rm.refund_date"
            return f"DATE_FORMAT({source_column}, '%Y-%m') AS stat_month"
        return None

    @staticmethod
    def _inventory_time_dimension_select(context: SemanticContext, side: str) -> str | None:
        if context.time_grain == "day":
            source_column = "inv.snapshot_date" if side == "snapshot" else "flow.flow_date"
            return f"{source_column} AS stat_date"
        if context.time_grain == "month":
            source_column = "inv.snapshot_date" if side == "snapshot" else "flow.flow_date"
            return f"DATE_FORMAT({source_column}, '%Y-%m') AS stat_month"
        return None

    @staticmethod
    def _time_dimension_group_by(context: SemanticContext, side: str) -> str | None:
        if context.time_grain == "day":
            return "om.order_date" if side == "order" else "rm.refund_date"
        if context.time_grain == "month":
            source_column = "om.order_date" if side == "order" else "rm.refund_date"
            return f"DATE_FORMAT({source_column}, '%Y-%m')"
        return None

    @staticmethod
    def _inventory_time_dimension_group_by(context: SemanticContext, side: str) -> str | None:
        if context.time_grain == "day":
            return "inv.snapshot_date" if side == "snapshot" else "flow.flow_date"
        if context.time_grain == "month":
            source_column = "inv.snapshot_date" if side == "snapshot" else "flow.flow_date"
            return f"DATE_FORMAT({source_column}, '%Y-%m')"
        return None

    def _inventory_snapshot_condition(self, context: SemanticContext) -> str:
        explicit_condition = self._current_time_condition("inv.snapshot_date", context)
        if explicit_condition:
            return explicit_condition
        return "inv.snapshot_date = (SELECT MAX(snapshot_date) FROM fct_inventory_snapshot)"

    def _inventory_flow_condition(self, context: SemanticContext) -> str:
        return self._current_time_condition("flow.flow_date", context)

    @staticmethod
    def _current_time_condition(column_name: str, context: SemanticContext) -> str:
        if context.time_window == "rolling_days" and context.time_window_value:
            return f"{column_name} >= CURDATE() - INTERVAL {context.time_window_value} DAY"
        if context.time_window == "yesterday":
            return f"{column_name} = CURDATE() - INTERVAL 1 DAY"
        if context.time_window == "today":
            return f"{column_name} = CURDATE()"
        if context.time_window == "this_month":
            return f"{column_name} >= DATE_FORMAT(CURDATE(), '%Y-%m-01')"
        if context.time_window == "last_month":
            return (
                f"{column_name} >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01') "
                f"AND {column_name} < DATE_FORMAT(CURDATE(), '%Y-%m-01')"
            )
        if context.time_window == "this_week":
            return f"{column_name} >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)"
        return ""

    @staticmethod
    def _previous_time_condition(column_name: str, context: SemanticContext) -> str:
        if context.compare_mode == "mom":
            if context.time_window == "rolling_days" and context.time_window_value:
                return (
                    f"{column_name} >= CURDATE() - INTERVAL {context.time_window_value * 2} DAY "
                    f"AND {column_name} < CURDATE() - INTERVAL {context.time_window_value} DAY"
                )
            if context.time_window == "yesterday":
                return f"{column_name} = CURDATE() - INTERVAL 2 DAY"
            if context.time_window == "today":
                return f"{column_name} = CURDATE() - INTERVAL 1 DAY"
            if context.time_window == "this_month":
                return (
                    f"{column_name} >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01') "
                    f"AND {column_name} < DATE_FORMAT(CURDATE(), '%Y-%m-01')"
                )
            if context.time_window == "last_month":
                return (
                    f"{column_name} >= DATE_FORMAT(CURDATE() - INTERVAL 2 MONTH, '%Y-%m-01') "
                    f"AND {column_name} < DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01')"
                )
            if context.time_window == "this_week":
                return (
                    f"{column_name} >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) "
                    f"AND {column_name} < DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)"
                )
        if context.compare_mode == "yoy":
            if context.time_window == "rolling_days" and context.time_window_value:
                return (
                    f"{column_name} >= DATE_SUB(CURDATE() - INTERVAL {context.time_window_value} DAY, INTERVAL 1 YEAR) "
                    f"AND {column_name} < DATE_SUB(CURDATE(), INTERVAL 1 YEAR)"
                )
            if context.time_window == "yesterday":
                return f"{column_name} = DATE_SUB(CURDATE() - INTERVAL 1 DAY, INTERVAL 1 YEAR)"
            if context.time_window == "today":
                return f"{column_name} = DATE_SUB(CURDATE(), INTERVAL 1 YEAR)"
            if context.time_window == "this_month":
                return (
                    f"{column_name} >= DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 1 YEAR) "
                    f"AND {column_name} < DATE_SUB(DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01'), INTERVAL 1 YEAR)"
                )
            if context.time_window == "last_month":
                return (
                    f"{column_name} >= DATE_SUB(DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01'), INTERVAL 1 YEAR) "
                    f"AND {column_name} < DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 1 YEAR)"
                )
            if context.time_window == "this_week":
                return (
                    f"{column_name} >= DATE_SUB(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY), INTERVAL 1 YEAR) "
                    f"AND {column_name} < DATE_SUB(DATE_ADD(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY), INTERVAL 7 DAY), INTERVAL 1 YEAR)"
                )
        return ""

    @staticmethod
    def _build_order_by(context: SemanticContext) -> str:
        sort_target = context.sort_metric
        if not sort_target:
            if context.time_grain == "day":
                return "\nORDER BY stat_date ASC"
            if context.time_grain == "month":
                return "\nORDER BY stat_month ASC"
            return ""
        direction = "DESC" if context.sort_desc else "ASC"
        return f"\nORDER BY {sort_target} {direction}"

    @staticmethod
    def _build_limit(context: SemanticContext) -> str:
        if not context.limit:
            return ""
        return f"\nLIMIT {context.limit}"
