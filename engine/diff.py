from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

import sqlglot

from .snapshot import TableSnapshot


@dataclass
class DiffResult:
    schema_sql: list[str]
    data_sql: list[str]
    warnings: list[str]


def _parse_columns_from_raw_ddl(raw_ddl: str) -> dict[str, str]:
    """
    Return {column_name: column_sql_definition} from a CREATE TABLE statement.
    Uses sqlglot AST rather than brittle text parsing.
    """
    try:
        expr = sqlglot.parse_one(raw_ddl, dialect="mysql")
    except Exception:
        return {}

    # sqlglot represents CREATE TABLE with `this` = table, `expressions` = columns/constraints
    cols: dict[str, str] = {}
    for col in expr.find_all(sqlglot.expressions.ColumnDef):
        name = col.this.name if hasattr(col.this, "name") else None
        if not name:
            continue
        cols[name] = col.sql(dialect="mysql")
    return cols


def _quote_ident(name: str) -> str:
    return f"`{name}`"


def _schema_diff_table(old: TableSnapshot | None, new: TableSnapshot | None) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    stmts: list[str] = []
    if old is None and new is not None:
        stmts.append(new.ddl_json["raw_ddl"])
        return stmts, warnings
    if old is not None and new is None:
        stmts.append(f"DROP TABLE IF EXISTS {_quote_ident(old.table_name)};")
        return stmts, warnings
    assert old is not None and new is not None

    old_raw = str(old.ddl_json.get("raw_ddl", ""))
    new_raw = str(new.ddl_json.get("raw_ddl", ""))
    old_cols = _parse_columns_from_raw_ddl(old_raw)
    new_cols = _parse_columns_from_raw_ddl(new_raw)
    if not old_cols or not new_cols:
        warnings.append(f"Could not parse DDL for table `{new.table_name}`; skipping schema diff.")
        return stmts, warnings

    old_set = set(old_cols.keys())
    new_set = set(new_cols.keys())
    added = sorted(new_set - old_set)
    removed = sorted(old_set - new_set)
    common = sorted(old_set & new_set)

    for c in added:
        stmts.append(
            f"ALTER TABLE {_quote_ident(new.table_name)} ADD COLUMN {new_cols[c]};"
        )
    for c in removed:
        stmts.append(
            f"ALTER TABLE {_quote_ident(new.table_name)} DROP COLUMN {_quote_ident(c)};"
        )
    for c in common:
        if old_cols[c] != new_cols[c]:
            # Type changes/renames are ambiguous; we still emit MODIFY COLUMN but warn.
            warnings.append(
                f"Column definition changed for `{new.table_name}`.`{c}`; review generated MODIFY COLUMN."
            )
            stmts.append(
                f"ALTER TABLE {_quote_ident(new.table_name)} MODIFY COLUMN {new_cols[c]};"
            )

    return stmts, warnings


def _pk_tuple(row: dict[str, Any], pk_columns: list[str]) -> tuple:
    return tuple(row.get(c) for c in pk_columns)


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    # Default: treat as string
    s = str(v).replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def _data_diff_table(old: TableSnapshot | None, new: TableSnapshot | None) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    stmts: list[str] = []
    if old is None and new is not None:
        # On create table, data is handled by schema+fresh snapshot; treat as inserts.
        if not new.pk_columns:
            warnings.append(f"Table `{new.table_name}` has no PK; skipping data diff.")
            return stmts, warnings
        for row in new.rows_json:
            cols = list(row.keys())
            vals = ", ".join(_sql_literal(row[c]) for c in cols)
            stmts.append(
                f"INSERT INTO {_quote_ident(new.table_name)} ({', '.join(_quote_ident(c) for c in cols)}) VALUES ({vals});"
            )
        return stmts, warnings
    if old is not None and new is None:
        # Drop table handled by schema diff
        return stmts, warnings
    assert old is not None and new is not None

    if not old.pk_columns or not new.pk_columns:
        warnings.append(f"Table `{new.table_name}` has no PK; skipping data diff.")
        return stmts, warnings
    if old.pk_columns != new.pk_columns:
        warnings.append(f"Primary key columns changed for `{new.table_name}`; skipping data diff.")
        return stmts, warnings

    pk_cols = new.pk_columns
    old_idx = {_pk_tuple(r, pk_cols): r for r in old.rows_json}
    new_idx = {_pk_tuple(r, pk_cols): r for r in new.rows_json}

    old_keys = set(old_idx.keys())
    new_keys = set(new_idx.keys())
    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    common = sorted(old_keys & new_keys)

    for k in added:
        row = new_idx[k]
        cols = list(row.keys())
        vals = ", ".join(_sql_literal(row[c]) for c in cols)
        stmts.append(
            f"INSERT INTO {_quote_ident(new.table_name)} ({', '.join(_quote_ident(c) for c in cols)}) VALUES ({vals});"
        )

    for k in removed:
        where = " AND ".join(f"{_quote_ident(c)} = {_sql_literal(val)}" for c, val in zip(pk_cols, k))
        stmts.append(f"DELETE FROM {_quote_ident(new.table_name)} WHERE {where};")

    for k in common:
        old_row = old_idx[k]
        new_row = new_idx[k]
        changed_cols = [c for c in new_row.keys() if old_row.get(c) != new_row.get(c)]
        if not changed_cols:
            continue
        set_clause = ", ".join(f"{_quote_ident(c)} = {_sql_literal(new_row.get(c))}" for c in changed_cols)
        where = " AND ".join(f"{_quote_ident(c)} = {_sql_literal(val)}" for c, val in zip(pk_cols, k))
        stmts.append(f"UPDATE {_quote_ident(new.table_name)} SET {set_clause} WHERE {where};")

    return stmts, warnings


def diff_snapshots(old: dict[str, TableSnapshot], new: dict[str, TableSnapshot]) -> DiffResult:
    schema_sql: list[str] = []
    data_sql: list[str] = []
    warnings: list[str] = []

    old_tables = set(old.keys())
    new_tables = set(new.keys())
    all_tables = sorted(old_tables | new_tables)

    # CREATE/ALTER first, DROP last
    create_alter: list[str] = []
    drops: list[str] = []
    for t in all_tables:
        s, w = _schema_diff_table(old.get(t), new.get(t))
        warnings.extend(w)
        for stmt in s:
            if stmt.strip().upper().startswith("DROP TABLE"):
                drops.append(stmt if stmt.endswith(";") else stmt + ";")
            else:
                create_alter.append(stmt if stmt.endswith(";") else stmt + ";")
    schema_sql = create_alter + drops

    for t in all_tables:
        s, w = _data_diff_table(old.get(t), new.get(t))
        warnings.extend(w)
        data_sql.extend(s)

    return DiffResult(schema_sql=schema_sql, data_sql=data_sql, warnings=warnings)


def load_snapshot_from_row(ddl_json: str, rows_json: str, table_name: str) -> TableSnapshot:
    ddl = json.loads(ddl_json)
    rows = json.loads(rows_json)
    pk_columns = ddl.get("pk_columns")
    if not pk_columns:
        # fallback: derive from columns metadata
        pk_columns = [c["name"] for c in ddl.get("columns", []) if c.get("key") == "PRI"]
    return TableSnapshot(
        table_name=table_name,
        ddl_json=ddl,
        rows_json=rows,
        row_count=len(rows),
        pk_columns=list(pk_columns),
    )

