from __future__ import annotations


TASK_MODE_LABELS = {
    "auto": "自动识别",
    "dql": "DQL 查询",
    "ads_sql": "ADS 建表写入",
    "ddl": "DDL 定义",
    "dml": "DML 变更",
    "dcl": "DCL 权限",
}

SQL_ENGINE_LABELS = {
    "mysql": "MySQL",
    "hql": "Hive SQL",
    "pg": "PostgreSQL",
    "oracle": "Oracle",
    "odpssql": "ODPS SQL",
    "sqlserver": "SQL Server",
}

QUERY_TASK_MODES = {"dql"}
NON_QUERY_TASK_MODES = {"ads_sql", "ddl", "dml", "dcl"}
SUPPORTED_SQL_ENGINES = tuple(SQL_ENGINE_LABELS)
SUPPORTED_TASK_MODES = tuple(TASK_MODE_LABELS)


def normalize_task_mode(task_mode: str | None) -> str | None:
    if not task_mode:
        return None
    value = task_mode.strip().lower()
    if value not in TASK_MODE_LABELS:
        raise ValueError(f"不支持的 SQL 类型: {task_mode}")
    if value == "auto":
        return None
    return value


def normalize_sql_engine(sql_engine: str | None) -> str:
    if not sql_engine:
        return "mysql"
    value = sql_engine.strip().lower()
    if value not in SQL_ENGINE_LABELS:
        raise ValueError(f"不支持的 SQL 语法: {sql_engine}")
    return value


def task_mode_label(task_mode: str) -> str:
    return TASK_MODE_LABELS.get(task_mode, task_mode.upper())


def sql_engine_label(sql_engine: str) -> str:
    return SQL_ENGINE_LABELS.get(sql_engine, sql_engine)


def is_query_task(task_mode: str) -> bool:
    return task_mode in QUERY_TASK_MODES

