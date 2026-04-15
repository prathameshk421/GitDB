from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import json

from .diff import DiffResult
from .errors import CheckoutDataError, CheckoutSchemaError
from .snapshot import TableSnapshot


def write_recovery_file(snapshot: dict[str, TableSnapshot], *, path: str) -> str:
    """
    Emit a best-effort recovery SQL file for manual restore.
    We store raw_ddl per table, plus data INSERTs for PK tables.
    """
    p = Path(path)
    lines: list[str] = []
    lines.append("-- GitDB recovery file")
    lines.append(f"-- Generated: {datetime.now().isoformat()}")
    lines.append("")
    for table, ts in snapshot.items():
        raw = ts.ddl_json.get("raw_ddl")
        if raw:
            lines.append(f"-- Schema for `{table}`")
            lines.append(str(raw).rstrip(";") + ";")
            lines.append("")
    # Data recovery omitted by default: schema restore is the critical escape hatch.
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def restore_schema_from_snapshot(conn, snapshot: dict[str, TableSnapshot]) -> None:
    cursor = conn.cursor()
    # Drop everything first to avoid conflicts; then recreate.
    cursor.execute("SELECT DATABASE()")
    _ = cursor.fetchone()
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
        """,
    )
    tables = [r[0] for r in cursor.fetchall()]
    for t in tables:
        cursor.execute(f"DROP TABLE IF EXISTS `{t}`")

    for table in sorted(snapshot.keys()):
        raw = snapshot[table].ddl_json.get("raw_ddl")
        if raw:
            cursor.execute(str(raw))


def apply_checkout(
    conn,
    *,
    diff: DiffResult,
    old_snapshot: dict[str, TableSnapshot],
    recovery_file_path: str | None = None,
) -> str | None:
    """
    Apply a two-phase checkout to the target DB connection.
    Returns recovery file path if schema application failed.
    """
    cursor = conn.cursor()
    recovery_path: str | None = None

    # Phase 1: schema (DDL auto-commits in MySQL)
    for stmt in diff.schema_sql:
        try:
            cursor.execute(stmt)
        except Exception as e:
            try:
                restore_schema_from_snapshot(conn, old_snapshot)
            finally:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                recovery_path = write_recovery_file(
                    old_snapshot,
                    path=recovery_file_path or f".gitdb/recovery_{ts}.sql",
                )
            raise CheckoutSchemaError(str(e)) from e

    # Phase 2: data (transactional)
    try:
        cursor.execute("START TRANSACTION")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        for stmt in diff.data_sql:
            cursor.execute(stmt)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise CheckoutDataError(str(e)) from e

    return recovery_path

