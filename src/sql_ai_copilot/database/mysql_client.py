from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import pymysql
from pymysql.cursors import DictCursor

from sql_ai_copilot.config.settings import MySQLSettings
from sql_ai_copilot.logging_utils import get_logger


class MySQLClient:
    def __init__(self, settings: MySQLSettings) -> None:
        self.settings = settings
        self.connection: pymysql.connections.Connection | None = None
        self.logger = get_logger("mysql")

    def _connect(self, use_database: bool = True) -> pymysql.connections.Connection:
        params = {
            "host": self.settings.host,
            "port": self.settings.port,
            "user": self.settings.user,
            "password": self.settings.password,
            "charset": self.settings.charset,
            "cursorclass": DictCursor,
            "autocommit": False,
        }
        if use_database:
            params["database"] = self.settings.database
        return pymysql.connect(**params)

    def ensure_database(self) -> None:
        self.logger.info("mysql_ensure_database_start %s", self.settings.database)
        connection = self._connect(use_database=False)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self.settings.database}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
                )
            connection.commit()
        finally:
            connection.close()
        self.logger.info("mysql_ensure_database_done %s", self.settings.database)

    def open(self) -> None:
        self.logger.info("mysql_open_start host=%s port=%s database=%s", self.settings.host, self.settings.port, self.settings.database)
        try:
            self.connection = self._connect(use_database=True)
        except pymysql.err.OperationalError as exc:
            if exc.args and exc.args[0] == 1049:
                self.ensure_database()
                self.connection = self._connect(use_database=True)
            else:
                raise
        self.logger.info("mysql_open_done database=%s", self.settings.database)

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self.logger.info("mysql_close_done database=%s", self.settings.database)

    @contextmanager
    def cursor(self) -> Iterator[DictCursor]:
        if self.connection is None:
            raise RuntimeError("MySQL 连接尚未打开。")
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(self, sql: str, parameters: tuple[Any, ...] | list[Any] | None = None) -> int:
        self.logger.info("mysql_execute %s", sql)
        with self.cursor() as cursor:
            affected = cursor.execute(sql, parameters)
        self.logger.info("mysql_execute_done affected=%s", affected)
        return affected

    def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> int:
        if not rows:
            return 0
        self.logger.info("mysql_executemany sql=%s rows=%s", sql, len(rows))
        with self.cursor() as cursor:
            affected = cursor.executemany(sql, rows)
        self.logger.info("mysql_executemany_done affected=%s", affected)
        return affected

    def query(self, sql: str, parameters: tuple[Any, ...] | list[Any] | None = None) -> list[dict[str, Any]]:
        self.logger.info("mysql_query %s", sql)
        with self.cursor() as cursor:
            cursor.execute(sql, parameters)
            rows = list(cursor.fetchall())
        self.logger.info("mysql_query_done rows=%s", len(rows))
        return rows

    def execute_script(self, sql_script: str) -> None:
        statements = [statement.strip() for statement in sql_script.split(";") if statement.strip()]
        with self.cursor() as cursor:
            for statement in statements:
                self.logger.info("mysql_execute_script_stmt %s", statement)
                cursor.execute(statement)

    def commit(self) -> None:
        if self.connection is None:
            raise RuntimeError("MySQL 连接尚未打开。")
        self.connection.commit()
        self.logger.info("mysql_commit_done")

    def rollback(self) -> None:
        if self.connection is not None:
            self.connection.rollback()
            self.logger.info("mysql_rollback_done")

    def __enter__(self) -> "MySQLClient":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc:
            self.rollback()
        else:
            self.commit()
        self.close()
