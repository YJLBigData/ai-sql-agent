from __future__ import annotations

from pathlib import Path

from .mysql_client import MySQLClient


class SchemaService:
    def __init__(self, client: MySQLClient, database_name: str) -> None:
        self.client = client
        self.database_name = database_name

    def create_tables_from_file(self, ddl_file: Path) -> None:
        self.client.execute_script(ddl_file.read_text(encoding="utf-8"))

    def get_schema_summary(self, table_names: list[str] | None = None) -> str:
        snapshot = self.get_schema_snapshot(table_names)
        return self._render_schema_snapshot(snapshot)

    def get_schema_snapshot(self, table_names: list[str] | None = None) -> dict[str, dict[str, object]]:
        sql = """
            SELECT
                c.table_name,
                t.table_comment,
                c.column_name,
                c.column_type,
                c.column_comment
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON c.table_schema = t.table_schema
             AND c.table_name = t.table_name
            WHERE c.table_schema = %s
        """
        parameters: list[object] = [self.database_name]
        if table_names:
            placeholders = ", ".join(["%s"] * len(table_names))
            sql += f" AND c.table_name IN ({placeholders})"
            parameters.extend(table_names)
        sql += " ORDER BY c.table_name, c.ordinal_position"
        rows = self.client.query(sql, tuple(parameters))
        grouped: dict[str, dict[str, object]] = {}
        for row in rows:
            table_name = row.get("table_name") or row.get("TABLE_NAME")
            table_comment = row.get("table_comment") or row.get("TABLE_COMMENT") or ""
            column_name = row.get("column_name") or row.get("COLUMN_NAME")
            column_type = row.get("column_type") or row.get("COLUMN_TYPE")
            column_comment = row.get("column_comment") or row.get("COLUMN_COMMENT") or ""
            if not table_name or not column_name or not column_type:
                continue
            table_info = grouped.setdefault(table_name, {"comment": table_comment, "columns": []})
            table_info["columns"].append(
                {
                    "name": column_name,
                    "type": column_type,
                    "comment": column_comment,
                }
            )
        return grouped

    def get_compact_schema_summary(
        self,
        relevant_columns: dict[str, tuple[str, ...]] | None = None,
        table_names: list[str] | None = None,
    ) -> str:
        snapshot = self.get_schema_snapshot(table_names)
        if not relevant_columns:
            return self._render_schema_snapshot(snapshot)

        compact_snapshot: dict[str, dict[str, object]] = {}
        for table_name, table_info in snapshot.items():
            selected_columns = set(relevant_columns.get(table_name, ()))
            if not selected_columns:
                continue
            selected_columns.update({"order_id", "refund_id", "store_id", "user_id", "product_id"})
            compact_columns = [
                column
                for column in table_info["columns"]
                if column["name"] in selected_columns
            ]
            if compact_columns:
                compact_snapshot[table_name] = {"comment": table_info["comment"], "columns": compact_columns}

        return self._render_schema_snapshot(compact_snapshot or snapshot)

    @staticmethod
    def _render_schema_snapshot(snapshot: dict[str, dict[str, object]]) -> str:
        lines: list[str] = []
        for table_name, table_info in snapshot.items():
            lines.append(f"Table {table_name} COMMENT '{table_info['comment']}'")
            rendered_columns = [
                f"{column['name']} {column['type']} COMMENT '{column['comment']}'"
                for column in table_info["columns"]
            ]
            lines.append("Columns: " + ", ".join(rendered_columns))
        return "\n".join(lines)

    def get_time_columns(self, table_names: list[str] | None = None) -> dict[str, list[dict[str, str]]]:
        sql = """
            SELECT
                table_name,
                column_name,
                column_type,
                column_comment
            FROM information_schema.columns
            WHERE table_schema = %s
              AND (
                    data_type IN ('date', 'datetime', 'timestamp', 'time')
                 OR column_name REGEXP '(_date|_time|dt|day)$'
              )
        """
        parameters: list[object] = [self.database_name]
        if table_names:
            placeholders = ", ".join(["%s"] * len(table_names))
            sql += f" AND table_name IN ({placeholders})"
            parameters.extend(table_names)
        sql += " ORDER BY table_name, ordinal_position"

        rows = self.client.query(sql, tuple(parameters))
        grouped: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            table_name = row.get("table_name") or row.get("TABLE_NAME")
            column_name = row.get("column_name") or row.get("COLUMN_NAME")
            column_type = row.get("column_type") or row.get("COLUMN_TYPE") or ""
            column_comment = row.get("column_comment") or row.get("COLUMN_COMMENT") or ""
            if not table_name or not column_name:
                continue
            grouped.setdefault(table_name, []).append(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "column_type": column_type,
                    "column_comment": column_comment,
                }
            )
        return grouped
