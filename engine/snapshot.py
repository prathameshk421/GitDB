from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Any

from .errors import SnapshotRowLimitError


@dataclass
class TableSnapshot:
    table_name: str
    ddl_json: dict
    rows_json: list[dict]
    row_count: int
    pk_columns: list[str]


def _fetchall_dict(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _get_tables(cursor, db_name: str) -> list[str]:
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """,
        (db_name,),
    )
    return [r[0] for r in cursor.fetchall()]


def _get_pk_columns(cursor, db_name: str, table: str) -> list[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
        """,
        (db_name, table),
    )
    return [r[0] for r in cursor.fetchall()]


def _get_columns(cursor, db_name: str, table: str) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (db_name, table),
    )
    cols = []
    for (name, col_type, is_nullable, default, key, extra) in cursor.fetchall():
        cols.append(
            {
                "name": name,
                "type": col_type,
                "nullable": (is_nullable == "YES"),
                "key": key or "",
                "default": default,
                "extra": extra or "",
            }
        )
    return cols


def _get_raw_ddl(cursor, table: str) -> str:
    cursor.execute(f"SHOW CREATE TABLE `{table}`")
    row = cursor.fetchone()
    # MySQL returns (table_name, create_statement)
    return row[1]


def _count_rows(cursor, table: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
    return int(cursor.fetchone()[0])


def _fetch_rows(cursor, table: str, pk_columns: list[str]) -> list[dict]:
    order_by = ", ".join(f"`{c}`" for c in pk_columns)
    cursor.execute(f"SELECT * FROM `{table}` ORDER BY {order_by}")
    return _fetchall_dict(cursor)


def capture_snapshot(conn, db_name: str, *, row_limit: int = 10_000) -> dict[str, TableSnapshot]:
    """
    Capture schema+data snapshot of the target database.
    Tables without a primary key have their DDL captured but rows are not.
    """
    cursor = conn.cursor()
    snapshot: dict[str, TableSnapshot] = {}
    for table in _get_tables(cursor, db_name):
        pk_cols = _get_pk_columns(cursor, db_name, table)
        ddl_json = {
            "table_name": table,
            "columns": _get_columns(cursor, db_name, table),
            "raw_ddl": _get_raw_ddl(cursor, table),
        }

        if not pk_cols:
            snapshot[table] = TableSnapshot(
                table_name=table,
                ddl_json=ddl_json,
                rows_json=[],
                row_count=0,
                pk_columns=[],
            )
            continue

        row_count = _count_rows(cursor, table)
        if row_count > row_limit:
            raise SnapshotRowLimitError(
                f"Table `{table}` has {row_count} rows (limit {row_limit})."
            )
        rows = _fetch_rows(cursor, table, pk_cols)
        snapshot[table] = TableSnapshot(
            table_name=table,
            ddl_json=ddl_json,
            rows_json=rows,
            row_count=row_count,
            pk_columns=pk_cols,
        )

    return snapshot


def snapshot_to_json(snapshot: dict[str, TableSnapshot]) -> str:
    # Deterministic serialization
    data = {k: asdict(v) for k, v in snapshot.items()}
    return json.dumps(data, sort_keys=True, default=str)

