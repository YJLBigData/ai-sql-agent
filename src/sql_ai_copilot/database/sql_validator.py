from __future__ import annotations

import re

from sql_ai_copilot.logging_utils import get_logger
from sql_ai_copilot.sql_meta import is_query_task, sql_engine_label

from .mysql_client import MySQLClient


ACTION_KEYWORDS = (
    "select",
    "insert",
    "update",
    "delete",
    "merge",
    "create",
    "alter",
    "drop",
    "truncate",
    "rename",
    "comment",
    "grant",
    "revoke",
)

TASK_ALLOWED_ACTIONS = {
    "dql": {"select"},
    "ddl": {"create", "alter", "drop", "truncate", "rename", "comment"},
    "dml": {"insert", "update", "delete", "merge"},
    "dcl": {"grant", "revoke"},
}

ENGINE_UNSUPPORTED_RULES = {
    "mysql": {
        re.compile(r"\bfull\s+outer\s+join\b", flags=re.IGNORECASE): "MySQL 不支持 FULL OUTER JOIN，请改成 LEFT JOIN / RIGHT JOIN / UNION ALL 方案。",
        re.compile(r"\bqualify\b", flags=re.IGNORECASE): "MySQL 不支持 QUALIFY 语法。",
        re.compile(r"\bilike\b", flags=re.IGNORECASE): "MySQL 不支持 ILIKE，请改成 LIKE。",
        re.compile(r"::"): "MySQL 不支持 :: 类型转换，请改用 CAST()。",
        re.compile(r"\bdateadd\b", flags=re.IGNORECASE): "MySQL 不支持 DATEADD，请改用 DATE_ADD。",
        re.compile(r"\bpartitioned\s+by\b", flags=re.IGNORECASE): "MySQL 不支持 PARTITIONED BY，请改用 PARTITION BY。",
        re.compile(r"\blifecycle\b", flags=re.IGNORECASE): "MySQL 不支持 LIFECYCLE 语法。",
    },
    "hql": {
        re.compile(r"\bauto_increment\b", flags=re.IGNORECASE): "Hive SQL 不支持 AUTO_INCREMENT。",
        re.compile(r"\bengine\s*=", flags=re.IGNORECASE): "Hive SQL 不支持 ENGINE 选项。",
        re.compile(r"::"): "Hive SQL 不支持 :: 类型转换。",
    },
    "pg": {
        re.compile(r"`"): "PostgreSQL 不支持反引号，请使用双引号或不加引号。",
        re.compile(r"\bauto_increment\b", flags=re.IGNORECASE): "PostgreSQL 不支持 AUTO_INCREMENT，请改用 IDENTITY 或 SERIAL。",
        re.compile(r"\bengine\s*=", flags=re.IGNORECASE): "PostgreSQL 不支持 ENGINE 选项。",
        re.compile(r"\blifecycle\b", flags=re.IGNORECASE): "PostgreSQL 不支持 LIFECYCLE 语法。",
        re.compile(r"\bpartitioned\s+by\b", flags=re.IGNORECASE): "PostgreSQL 不支持 PARTITIONED BY，请改用 PARTITION BY。",
        re.compile(r"\bdistribute\s+by\b", flags=re.IGNORECASE): "PostgreSQL 不支持 DISTRIBUTE BY。",
    },
    "oracle": {
        re.compile(r"`"): "Oracle 不支持反引号。",
        re.compile(r"\blimit\b", flags=re.IGNORECASE): "Oracle 不支持 LIMIT，请改用 FETCH FIRST 或 ROWNUM。",
        re.compile(r"\bauto_increment\b", flags=re.IGNORECASE): "Oracle 不支持 AUTO_INCREMENT。",
        re.compile(r"\blifecycle\b", flags=re.IGNORECASE): "Oracle 不支持 LIFECYCLE 语法。",
        re.compile(r"\bpartitioned\s+by\b", flags=re.IGNORECASE): "Oracle 不支持 Hive/ODPS 风格的 PARTITIONED BY。",
    },
    "odpssql": {
        re.compile(r"\bauto_increment\b", flags=re.IGNORECASE): "ODPS SQL 不支持 AUTO_INCREMENT。",
        re.compile(r"`"): "ODPS SQL 不支持反引号。",
        re.compile(r"\bengine\s*=", flags=re.IGNORECASE): "ODPS SQL 不支持 ENGINE 选项。",
        re.compile(r"\bserial\b", flags=re.IGNORECASE): "ODPS SQL 不支持 SERIAL。",
    },
    "sqlserver": {
        re.compile(r"`"): "SQL Server 不支持反引号。",
        re.compile(r"\blimit\b", flags=re.IGNORECASE): "SQL Server 不支持 LIMIT，请改用 TOP 或 OFFSET FETCH。",
        re.compile(r"\bauto_increment\b", flags=re.IGNORECASE): "SQL Server 不支持 AUTO_INCREMENT。",
        re.compile(r"\bserial\b", flags=re.IGNORECASE): "SQL Server 不支持 SERIAL。",
        re.compile(r"\blifecycle\b", flags=re.IGNORECASE): "SQL Server 不支持 LIFECYCLE 语法。",
        re.compile(r"\bpartitioned\s+by\b", flags=re.IGNORECASE): "SQL Server 不支持 Hive/ODPS 风格的 PARTITIONED BY。",
    },
}


class SQLValidator:
    def __init__(self, client: MySQLClient) -> None:
        self.client = client
        self.logger = get_logger("validator")

    def validate(self, sql: str, task_mode: str, sql_engine: str) -> None:
        normalized = sql.strip()
        if not normalized:
            raise ValueError("SQL 不能为空。")
        if task_mode == "ads_sql":
            self.validate_ads_sql(normalized, sql_engine)
            return
        if is_query_task(task_mode):
            self.validate_dql(normalized, sql_engine)
            return
        self.validate_non_query(normalized, task_mode, sql_engine)

    def validate_dql(self, sql: str, sql_engine: str) -> None:
        statements = self._split_statements(sql)
        if len(statements) != 1:
            raise ValueError("DQL 只允许输出一段查询 SQL。")
        statement = statements[0]
        self._check_engine_compatibility(statement, sql_engine)
        action = self._detect_action(statement)
        if action != "select":
            raise ValueError("DQL 只允许生成 SELECT / WITH 查询语句。")
        if sql_engine != "mysql":
            raise ValueError("当前 DQL 查询只支持 MySQL 语法与执行校验。")
        self.client.query(f"EXPLAIN {statement}")

    def validate_ads_sql(self, sql: str, sql_engine: str) -> None:
        statements = self._split_statements(sql)
        if len(statements) != 2:
            raise ValueError("ADS 建表写入模式必须输出两段 SQL：CREATE TABLE 和 INSERT INTO ... SELECT ...")
        create_stmt, insert_stmt = statements
        self._check_engine_compatibility(create_stmt, sql_engine)
        self._check_engine_compatibility(insert_stmt, sql_engine)

        if self._detect_action(create_stmt) != "create":
            raise ValueError("ADS 第一段 SQL 必须是 CREATE TABLE。")
        if self._detect_action(insert_stmt) != "insert":
            raise ValueError("ADS 第二段 SQL 必须是 INSERT INTO ... SELECT ...")
        if not re.search(r"\bselect\b", insert_stmt, flags=re.IGNORECASE):
            raise ValueError("ADS 第二段 SQL 必须包含 SELECT。")

        if sql_engine == "mysql":
            select_sql = self._extract_insert_select(insert_stmt)
            self.logger.info("validate_ads_select %s", select_sql)
            self.client.query(f"EXPLAIN {select_sql}")

    def validate_non_query(self, sql: str, task_mode: str, sql_engine: str) -> None:
        statements = self._split_statements(sql)
        if not statements:
            raise ValueError("SQL 不能为空。")
        allowed_actions = TASK_ALLOWED_ACTIONS.get(task_mode)
        if not allowed_actions:
            raise ValueError(f"暂不支持的 SQL 类型: {task_mode}")

        for statement in statements:
            self._check_engine_compatibility(statement, sql_engine)
            action = self._detect_action(statement)
            if action not in allowed_actions:
                raise ValueError(
                    f"{task_mode.upper()} 只允许生成 {', '.join(sorted(allowed_actions)).upper()} 语句，"
                    f"当前检测到 {action.upper()}。"
                )

    def _check_engine_compatibility(self, sql: str, sql_engine: str) -> None:
        engine_rules = ENGINE_UNSUPPORTED_RULES.get(sql_engine, {})
        for pattern, message in engine_rules.items():
            if pattern.search(sql):
                self.logger.warning("sql_incompatible engine=%s message=%s", sql_engine, message)
                raise ValueError(message)

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        statements: list[str] = []
        current: list[str] = []
        in_single_quote = False
        in_double_quote = False

        for char in sql:
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

            if char == ";" and not in_single_quote and not in_double_quote:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                continue
            current.append(char)

        tail = "".join(current).strip()
        if tail:
            statements.append(tail)
        return statements

    def _detect_action(self, statement: str) -> str:
        cleaned = statement.strip().lstrip("(").strip()
        lowered = cleaned.lower()
        if lowered.startswith("with"):
            action = self._detect_action_after_cte(cleaned)
            if action:
                return action
        match = re.match(r"([a-z]+)", lowered)
        if not match:
            raise ValueError("无法识别 SQL 语句类型。")
        action = match.group(1)
        if action not in ACTION_KEYWORDS:
            raise ValueError(f"不支持的 SQL 语句类型: {action.upper()}。")
        return action

    @staticmethod
    def _detect_action_after_cte(statement: str) -> str | None:
        depth = 0
        lowered = statement.lower()
        index = 0
        while index < len(lowered):
            char = lowered[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif depth == 0:
                for keyword in ACTION_KEYWORDS:
                    if lowered.startswith(keyword, index) and (index == 0 or not lowered[index - 1].isalnum()):
                        if keyword != "with":
                            return keyword
            index += 1
        return None

    def _extract_insert_select(self, insert_stmt: str) -> str:
        lowered = insert_stmt.lower()
        with_index = lowered.find("with")
        select_index = lowered.find("select")
        if with_index >= 0 and (select_index < 0 or with_index < select_index):
            return insert_stmt[with_index:].strip()
        if select_index >= 0:
            return insert_stmt[select_index:].strip()
        raise ValueError(f"{sql_engine_label('mysql')} INSERT INTO 语句必须包含 SELECT。")
