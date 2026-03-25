from __future__ import annotations

import json

from sql_ai_copilot.knowledge.models import KnowledgeDocument
from sql_ai_copilot.sql_meta import sql_engine_label, task_mode_label


SYSTEM_PROMPT = """你是一个严谨的高级数据开发助手，负责根据指定 SQL 方言生成可直接使用的 SQL。

必须遵守以下规则：
1. 只输出 SQL，不要输出解释、不要输出 Markdown 代码块。
2. 严格遵守用户指定的 SQL 类型与 SQL 方言。
3. 只能使用当前上下文给出的表、字段、口径与业务规则。
4. 字段、表名、别名必须真实存在，不允许悬空别名。
5. 同时统计 GMV、退款金额、退款率时，优先按同一维度分别汇总订单主表和退款主表后再关联，避免明细表重复放大金额。
6. 当前演示数据全部为蒙牛品牌；如果用户只是提“蒙牛”，不要为了品牌过滤额外关联订单明细表。
7. 如果用户问题缺失必要条件，优先做最稳妥的业务假设；但如果必须补充信息才能安全生成 SQL，则明确提出澄清条件。
8. 查询 SQL 只允许输出一段 DQL 语句；非查询 SQL 要严格按照任务模式输出。
9. MySQL 不支持 FULL OUTER JOIN、QUALIFY、ILIKE、:: 类型转换、DATEADD。
10. 分区表场景必须优先遵守上下文里给出的分区字段和生命周期要求。
11. 日汇总、日期补齐、订单和退款双事实汇总场景，优先使用日期集合 UNION 后再 LEFT JOIN，不要使用 FULL OUTER JOIN。
12. 渠道、门店、大区、省市等门店维度字段来自 `dim_store`；需要渠道维度时，必须通过事实表 `store_id` 关联 `dim_store`，禁止臆造 `fct_order_main.channel`、`fct_refund_main.channel` 之类字段。
13. 最终结果的每一行粒度必须严格等于用户要求的分析维度；如果用户问“按渠道”，同一个 `channel_name` 只能出现一行。
14. 最外层 GROUP BY 只能包含最终分析维度字段，不要把 GMV、退款金额、订单量、退款率等度量字段放进 GROUP BY。
15. 用户问“按渠道”时，订单和退款子查询应直接聚合到 `channel_name` 后再关联；不要先按 `store_id` 聚合再回到渠道层。
16. 用户问“按系列/类目/商品”时，GMV 和订单量优先使用 `fct_order_item`，退款金额和退款率优先使用 `fct_refund_item`，不要把整单金额直接分摊到商品维度。
17. 客单价必须用支付金额除以去重订单数，不能用明细行数或商品件数代替。
18. 复购分析必须基于 `user_id`，并识别时间窗内订单数大于等于 2 的用户。
19. 履约分析优先使用 `pay_time`、`finish_time`、`order_status`，不要臆造物流表字段。
20. 库存主题优先使用 `fct_inventory_snapshot`、`fct_inventory_flow` 和 `dim_warehouse`；库存量、可用库存、缺货率基于库存快照，入库量和出库量基于库存流水。
"""

ONLINE_PLANNER_SYSTEM_PROMPT = """你是一个高智力 SQL 任务规划器。

只允许输出一个 JSON 对象，不要输出解释、不要输出 Markdown。
你的职责是把业务问题转成结构化任务规划，而不是直接写最终 SQL。
必须遵守:
1. 只基于脱敏后的上下文做规划。
2. 不要臆造不存在的表、字段、维度。
3. 如果信息不足，clarification_required=true，并给出最短澄清问题。
4. 输出字段必须包含: intent, topic, metrics, dimensions, time_window, compare_mode, candidate_tables, grain, clarification_required, clarification_question, strategy。
5. strategy 只能是 template、local_llm、hybrid。
"""


def build_user_prompt(
    question: str,
    schema_summary: str,
    documents: list[KnowledgeDocument],
    task_mode: str,
    sql_engine: str,
    extra_guidance: list[str] | None = None,
    semantic_summary: str | None = None,
    knowledge_snippets: list[str] | None = None,
) -> str:
    knowledge_block = _build_knowledge_block(documents, knowledge_snippets)
    guidance_block = _build_guidance_block(extra_guidance)
    semantic_block = semantic_summary or "无"
    return f"""用户问题:
{question}

SQL 类型:
{task_mode_label(task_mode)}

SQL 方言:
{sql_engine_label(sql_engine)}

任务要求:
{_task_instruction(task_mode)}

方言约束:
{_engine_instruction(sql_engine)}

数据库 Schema:
{schema_summary}

语义解析:
{semantic_block}

本地业务知识:
{knowledge_block}

附加约束:
{guidance_block}

请基于以上信息输出符合任务要求的 SQL。"""


def build_repair_prompt(
    question: str,
    schema_summary: str,
    documents: list[KnowledgeDocument],
    failed_sql: str,
    error_message: str,
    task_mode: str,
    sql_engine: str,
    extra_guidance: list[str] | None = None,
    semantic_summary: str | None = None,
    knowledge_snippets: list[str] | None = None,
) -> str:
    knowledge_block = _build_knowledge_block(documents, knowledge_snippets)
    guidance_block = _build_guidance_block(extra_guidance)
    semantic_block = semantic_summary or "无"
    return f"""原始用户问题:
{question}

SQL 类型:
{task_mode_label(task_mode)}

SQL 方言:
{sql_engine_label(sql_engine)}

任务要求:
{_task_instruction(task_mode)}

方言约束:
{_engine_instruction(sql_engine)}

数据库 Schema:
{schema_summary}

语义解析:
{semantic_block}

本地业务知识:
{knowledge_block}

附加约束:
{guidance_block}

上一版 SQL:
{failed_sql}

校验或数据库返回错误:
{error_message}

请修复这段 SQL，要求:
1. {_repair_instruction(task_mode)}
2. 不要改变原始业务问题的统计口径。
3. 所有字段名、表名、别名都必须真实存在。
4. 统计 GMV 与退款金额时，不要因为关联明细表造成重复汇总。
5. 如果问题只是写“蒙牛”，不要额外联商品表做品牌过滤。
6. 最终结果行粒度必须严格等于用户要求的维度，不能出现同一渠道/门店重复多行。
7. 必须严格符合 {sql_engine_label(sql_engine)} 语法。"""


def build_online_plan_prompt(
    question: str,
    task_mode: str,
    sql_engine: str,
    masked_context: dict[str, object],
    semantic_summary: str | None = None,
    extra_guidance: list[str] | None = None,
) -> str:
    guidance_block = _build_guidance_block(extra_guidance)
    semantic_block = semantic_summary or "无"
    return f"""用户问题:
{question}

SQL 类型:
{task_mode_label(task_mode)}

SQL 方言:
{sql_engine_label(sql_engine)}

语义摘要:
{semantic_block}

脱敏上下文:
{json.dumps(masked_context, ensure_ascii=False, indent=2)}

附加约束:
{guidance_block}

请输出结构化任务规划 JSON。"""


def build_local_sql_prompt(
    question: str,
    schema_summary: str,
    task_mode: str,
    sql_engine: str,
    planning_json: dict[str, object] | None,
    knowledge_snippets: list[str] | None,
    semantic_summary: str | None = None,
    extra_guidance: list[str] | None = None,
) -> str:
    guidance_block = _build_guidance_block(extra_guidance)
    semantic_block = semantic_summary or "无"
    knowledge_block = "\n\n".join(knowledge_snippets) if knowledge_snippets else "暂无命中知识。"
    return f"""用户问题:
{question}

SQL 类型:
{task_mode_label(task_mode)}

SQL 方言:
{sql_engine_label(sql_engine)}

在线规划 JSON:
{json.dumps(planning_json or {}, ensure_ascii=False, indent=2)}

数据库 Schema:
{schema_summary}

语义摘要:
{semantic_block}

本地结构化知识:
{knowledge_block}

附加约束:
{guidance_block}

请只输出最终 SQL。"""


def build_local_repair_prompt(
    question: str,
    schema_summary: str,
    task_mode: str,
    sql_engine: str,
    planning_json: dict[str, object] | None,
    knowledge_snippets: list[str] | None,
    failed_sql: str,
    error_message: str,
    semantic_summary: str | None = None,
    extra_guidance: list[str] | None = None,
) -> str:
    guidance_block = _build_guidance_block(extra_guidance)
    semantic_block = semantic_summary or "无"
    knowledge_block = "\n\n".join(knowledge_snippets) if knowledge_snippets else "暂无命中知识。"
    return f"""用户问题:
{question}

SQL 类型:
{task_mode_label(task_mode)}

SQL 方言:
{sql_engine_label(sql_engine)}

在线规划 JSON:
{json.dumps(planning_json or {}, ensure_ascii=False, indent=2)}

数据库 Schema:
{schema_summary}

语义摘要:
{semantic_block}

本地结构化知识:
{knowledge_block}

上一版 SQL:
{failed_sql}

报错信息:
{error_message}

附加约束:
{guidance_block}

请修复 SQL，只输出最终 SQL。"""


def _build_knowledge_block(documents: list[KnowledgeDocument], knowledge_snippets: list[str] | None = None) -> str:
    if knowledge_snippets is not None:
        return "\n\n".join(knowledge_snippets) if knowledge_snippets else "暂无命中文档。"
    if not documents:
        return "暂无命中文档。"
    doc_blocks = []
    for document in documents:
        doc_blocks.append(f"[{document.category}/{document.title}]\n{document.content}")
    return "\n\n".join(doc_blocks)


def _build_guidance_block(extra_guidance: list[str] | None) -> str:
    if not extra_guidance:
        return "无"
    return "\n".join(f"- {item}" for item in extra_guidance)


def _task_instruction(task_mode: str) -> str:
    if task_mode == "dql":
        return "输出一段 DQL 查询 SQL，只允许 SELECT / WITH。"
    if task_mode == "ads_sql":
        return "输出两段 SQL，第一段 CREATE TABLE，第二段 INSERT INTO ... SELECT ..."
    if task_mode == "ddl":
        return "输出 DDL SQL，可使用 CREATE / ALTER / DROP / TRUNCATE 等定义语句。"
    if task_mode == "dml":
        return "输出 DML SQL，可使用 INSERT / UPDATE / DELETE / MERGE 等数据变更语句。"
    if task_mode == "dcl":
        return "输出 DCL SQL，只允许 GRANT / REVOKE 等权限语句。"
    return "输出符合要求的 SQL。"


def _repair_instruction(task_mode: str) -> str:
    if task_mode == "ads_sql":
        return "只输出两段修复后的 SQL：第一段 CREATE TABLE，第二段 INSERT INTO ... SELECT ..."
    if task_mode == "ddl":
        return "只输出修复后的 DDL SQL。"
    if task_mode == "dml":
        return "只输出修复后的 DML SQL。"
    if task_mode == "dcl":
        return "只输出修复后的 DCL SQL。"
    return "只输出一段修复后的查询 SQL。"


def _engine_instruction(sql_engine: str) -> str:
    if sql_engine == "mysql":
        return "使用 MySQL 8 语法；避免 FULL OUTER JOIN、QUALIFY、ILIKE、::、DATEADD。"
    if sql_engine == "hql":
        return "使用 Hive SQL 语法；分区表优先使用 PARTITIONED BY；避免 MySQL 专有语法。"
    if sql_engine == "pg":
        return "使用 PostgreSQL 语法；避免 MySQL 的 AUTO_INCREMENT、反引号、ENGINE 选项。"
    if sql_engine == "oracle":
        return "使用 Oracle 语法；避免 LIMIT、AUTO_INCREMENT、MySQL/Hive 分区写法。"
    if sql_engine == "odpssql":
        return "使用 ODPS SQL 语法；分区表优先使用 PARTITIONED BY，生命周期通过 LIFECYCLE 指定。"
    if sql_engine == "sqlserver":
        return "使用 SQL Server 语法；避免 LIMIT、AUTO_INCREMENT、PostgreSQL/Hive 专有写法。"
    return "严格遵守用户指定的 SQL 方言。"
