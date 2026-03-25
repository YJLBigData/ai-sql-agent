from __future__ import annotations


def build_default_evaluation_cases() -> list[dict[str, object]]:
    cases: list[dict[str, object]] = []
    case_no = 1

    sales_windows = ["近7天", "近30天", "本月", "上月"]
    sales_dimensions = [
        ("渠道", "channel_name", ["dim_store", "channel_name"]),
        ("门店", "store_name", ["dim_store", "store_name"]),
        ("系列", "series_name", ["series_name"]),
        ("类目", "category_name", ["category_name"]),
        ("品牌", "brand_name", ["brand_name"]),
    ]
    sales_topics = [
        ("GMV", ["payment_amount"], "统计{window}各{dimension}GMV，按GMV降序展示。"),
        ("净销售额", ["net_payment_amount"], "统计{window}各{dimension}净销售额，按净销售额降序展示。"),
        ("退款金额和退款率", ["refund_status", "refund_amount"], "统计{window}各{dimension}退款金额和退款率，按退款金额降序展示。"),
        ("客单价", ["count(distinct", "payment_amount"], "统计{window}各{dimension}客单价，按客单价降序展示。"),
    ]
    for window in sales_windows:
        for dimension_cn, _, hints in sales_dimensions:
            for metric_cn, sql_parts, template in sales_topics:
                question = template.format(window=window, dimension=dimension_cn)
                cases.append(
                    _case(
                        case_no,
                        question,
                        must_contain_sql=[*hints, *sql_parts],
                    )
                )
                case_no += 1

    product_windows = ["近14天", "近30天"]
    for window in product_windows:
        for dimension_cn, _, hints in [("系列", "series_name", ["series_name", "pay_amount"]), ("商品", "product_name", ["product_name", "pay_amount"])]:
            cases.append(_case(case_no, f"统计{window}各{dimension_cn}GMV Top 10。", must_contain_sql=hints))
            case_no += 1
            cases.append(_case(case_no, f"统计{window}各{dimension_cn}退款金额和退款率，按退款金额降序展示。", must_contain_sql=[*hints, "refund_amount"]))
            case_no += 1

    repeat_dims = [("渠道", ["channel_name", "user_id", ">= 2"]), ("门店", ["store_name", "user_id", ">= 2"])]
    for window in ["近30天", "近60天"]:
        for dimension_cn, hints in repeat_dims:
            cases.append(_case(case_no, f"统计{window}各{dimension_cn}复购率和复购人数，按复购率降序展示。", must_contain_sql=hints))
            case_no += 1

    for window in ["近7天", "近30天", "本月"]:
        for dimension_cn, hints in [("渠道", ["channel_name", "finish_time", "pay_time"]), ("门店", ["store_name", "finish_time", "pay_time"])]:
            cases.append(_case(case_no, f"统计{window}各{dimension_cn}履约率和平均履约时长，按履约率降序展示。", must_contain_sql=hints))
            case_no += 1

    for window in ["近30天", "本月"]:
        for dimension_cn, hints in [("渠道", ["channel_name", "current_gmv", "previous_gmv"]), ("系列", ["series_name", "current_gmv", "previous_gmv"])]:
            cases.append(_case(case_no, f"统计{window}各{dimension_cn}GMV环比，按环比降序展示。", must_contain_sql=hints))
            case_no += 1

    inventory_dims = [
        ("仓库", ["fct_inventory_snapshot", "warehouse_name"]),
        ("仓型", ["fct_inventory_snapshot", "warehouse_type"]),
        ("系列", ["fct_inventory_snapshot", "series_name"]),
        ("品牌", ["fct_inventory_snapshot", "brand_name"]),
    ]
    for dimension_cn, hints in inventory_dims:
        cases.append(_case(case_no, f"统计当前各{dimension_cn}库存量、可用库存和缺货率，按库存量降序展示。", must_contain_sql=[*hints, "available_qty"]))
        case_no += 1

    flow_dims = [("仓库", ["fct_inventory_flow", "warehouse_name"]), ("系列", ["fct_inventory_flow", "series_name"]), ("品牌", ["fct_inventory_flow", "brand_name"])]
    for window in ["近7天", "近14天", "近30天"]:
        for dimension_cn, hints in flow_dims:
            cases.append(_case(case_no, f"统计{window}各{dimension_cn}入库量和出库量，按出库量降序展示。", must_contain_sql=[*hints, "direction"]))
            case_no += 1

    ads_cases = [
        "帮我创建一张ads表，统计近30天各渠道GMV，并给出建表和insert语句。",
        "帮我创建一张ads表，统计近30天各系列退款金额和退款率，并给出建表和insert语句。",
        "帮我创建一张ads表，统计近30天各仓库库存量和可用库存，并给出建表和insert语句。",
        "帮我创建一张ads表，统计近14天各系列出入库量，并给出建表和insert语句。",
    ]
    for question in ads_cases:
        cases.append(
            _case(
                case_no,
                question,
                task_mode="ads_sql",
                execute=False,
                must_contain_sql=["create table", "insert into"],
                min_row_count=None,
            )
        )
        case_no += 1

    ddl_cases = [
        "帮我创建一个按天分区的ods订单表，字段包含订单号、支付金额、支付时间。",
        "帮我创建一个按天分区的库存快照表，字段包含仓库、商品、库存量、快照日期。",
        "帮我创建一个按天分区的退款汇总表，字段包含渠道、退款金额、退款率、dt。",
        "帮我创建一个按月分区的商品销售汇总表，字段包含品牌、系列、gmv、stat_month。",
    ]
    for question in ddl_cases:
        cases.append(_case(case_no, question, task_mode="ddl", execute=False, must_contain_sql=["create table"], min_row_count=None))
        case_no += 1

    dml_cases = [
        "帮我写一条把近30天各渠道GMV写入临时表的insert语句。",
        "帮我写一条把近30天各系列退款金额写入临时表的insert语句。",
        "帮我写一条把当前仓库库存汇总写入临时表的insert语句。",
        "帮我写一条更新门店渠道标签的update语句。",
    ]
    for question in dml_cases:
        keyword = "update" if "update" in question.lower() or "更新" in question else "insert"
        cases.append(_case(case_no, question, task_mode="dml", execute=False, must_contain_sql=[keyword], min_row_count=None))
        case_no += 1

    dcl_cases = [
        "帮我生成给分析师只读 dim_store 的 grant 语句。",
        "帮我生成回收分析师 dim_product 权限的 revoke 语句。",
    ]
    for question in dcl_cases:
        keyword = "grant" if "grant" in question.lower() or "授权" in question else "revoke"
        cases.append(_case(case_no, question, task_mode="dcl", execute=False, must_contain_sql=[keyword], min_row_count=None))
        case_no += 1

    while len(cases) < 100:
        question = f"统计近30天各渠道GMV和退款金额，按渠道降序展示。补充回归样本 {len(cases) + 1}。"
        cases.append(_case(case_no, question, must_contain_sql=["channel_name", "payment_amount", "refund_amount"]))
        case_no += 1

    return cases[:100]


def _case(
    case_no: int,
    question: str,
    task_mode: str = "dql",
    sql_engine: str = "mysql",
    engine_mode: str = "single",
    provider: str = "local",
    execute: bool = True,
    must_contain_sql: list[str] | None = None,
    min_row_count: int | None = None,
) -> dict[str, object]:
    return {
        "case_id": f"CASE_{case_no:03d}",
        "question": question,
        "task_mode": task_mode,
        "sql_engine": sql_engine,
        "engine_mode": engine_mode,
        "provider": provider,
        "execute": execute,
        "must_contain_sql": must_contain_sql or [],
        "min_row_count": min_row_count,
    }
