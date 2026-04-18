"""Unit tests for engine/diff.py – no live DB required."""
from __future__ import annotations

import pytest

from engine.diff import DiffResult, diff_snapshots, load_snapshot_from_row
from engine.snapshot import TableSnapshot


# ── Helpers ──────────────────────────────────────────────────────────────────

def _snap(
    table: str,
    raw_ddl: str,
    rows: list[dict],
    pk_cols: list[str],
) -> TableSnapshot:
    return TableSnapshot(
        table_name=table,
        ddl_json={"raw_ddl": raw_ddl},
        rows_json=rows,
        row_count=len(rows),
        pk_columns=pk_cols,
    )


USERS_DDL = "CREATE TABLE `users` (`id` INT PRIMARY KEY, `name` VARCHAR(50));"
USERS_DDL_EXTRA_COL = (
    "CREATE TABLE `users` (`id` INT PRIMARY KEY, `name` VARCHAR(50), `email` VARCHAR(100));"
)
USERS_DDL_REMOVED_COL = "CREATE TABLE `users` (`id` INT PRIMARY KEY);"


# ── Data Diff ────────────────────────────────────────────────────────────────

class TestDataDiff:
    def test_insert_update_delete(self):
        old = {"users": _snap("users", USERS_DDL, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}], ["id"])}
        new = {"users": _snap("users", USERS_DDL, [{"id": 1, "name": "alice2"}, {"id": 3, "name": "carol"}], ["id"])}

        res = diff_snapshots(old, new)
        sql = "\n".join(res.data_sql)

        assert "INSERT INTO `users`" in sql  # id=3 carol
        assert "UPDATE `users`" in sql        # id=1 name changed
        assert "DELETE FROM `users`" in sql   # id=2 removed

    def test_no_change_produces_no_sql(self):
        snap = {"users": _snap("users", USERS_DDL, [{"id": 1, "name": "alice"}], ["id"])}
        res = diff_snapshots(snap, snap)
        assert res.data_sql == []

    def test_new_table_with_rows_produces_inserts(self):
        old: dict = {}
        new = {"users": _snap("users", USERS_DDL, [{"id": 1, "name": "alice"}], ["id"])}
        res = diff_snapshots(old, new)
        sql = "\n".join(res.data_sql)
        assert "INSERT INTO `users`" in sql

    def test_dropped_table_data_produces_no_sql(self):
        """DROP TABLE in schema diff means data diff is a no-op – no deletes emitted."""
        old = {"users": _snap("users", USERS_DDL, [{"id": 1, "name": "alice"}], ["id"])}
        new: dict = {}
        res = diff_snapshots(old, new)
        # Data SQL should be empty; table removal is handled via DROP TABLE in schema
        delete_stmts = [s for s in res.data_sql if "DELETE" in s]
        assert delete_stmts == []

    def test_null_value_handling(self):
        old = {"t": _snap("t", "CREATE TABLE `t` (`id` INT PRIMARY KEY, `v` VARCHAR(10));", [{"id": 1, "v": "x"}], ["id"])}
        new = {"t": _snap("t", "CREATE TABLE `t` (`id` INT PRIMARY KEY, `v` VARCHAR(10));", [{"id": 1, "v": None}], ["id"])}
        res = diff_snapshots(old, new)
        sql = "\n".join(res.data_sql)
        assert "UPDATE `t`" in sql
        assert "NULL" in sql

    def test_no_pk_table_is_skipped_with_warning(self):
        old = {"t": _snap("t", "CREATE TABLE `t` (`name` VARCHAR(50));", [{"name": "x"}], [])}
        new = {"t": _snap("t", "CREATE TABLE `t` (`name` VARCHAR(50));", [{"name": "y"}], [])}
        res = diff_snapshots(old, new)
        assert res.data_sql == []
        assert any("no PK" in w or "no pk" in w.lower() for w in res.warnings)

    def test_composite_pk(self):
        old = {"t": _snap("t", "CREATE TABLE `t` (`a` INT, `b` INT, `v` INT, PRIMARY KEY(`a`,`b`));", [{"a": 1, "b": 1, "v": 10}], ["a", "b"])}
        new = {"t": _snap("t", "CREATE TABLE `t` (`a` INT, `b` INT, `v` INT, PRIMARY KEY(`a`,`b`));", [{"a": 1, "b": 1, "v": 99}], ["a", "b"])}
        res = diff_snapshots(old, new)
        sql = "\n".join(res.data_sql)
        assert "UPDATE `t`" in sql
        assert "`a` = 1" in sql
        assert "`b` = 1" in sql


# ── Schema Diff ──────────────────────────────────────────────────────────────

class TestSchemaDiff:
    def test_new_table_produces_create(self):
        old: dict = {}
        new = {"users": _snap("users", USERS_DDL, [], ["id"])}
        res = diff_snapshots(old, new)
        assert any("CREATE TABLE" in s for s in res.schema_sql)

    def test_dropped_table_produces_drop(self):
        old = {"users": _snap("users", USERS_DDL, [], ["id"])}
        new: dict = {}
        res = diff_snapshots(old, new)
        assert any("DROP TABLE" in s for s in res.schema_sql)

    def test_add_column_produces_alter_add(self):
        old = {"users": _snap("users", USERS_DDL, [], ["id"])}
        new = {"users": _snap("users", USERS_DDL_EXTRA_COL, [], ["id"])}
        res = diff_snapshots(old, new)
        sql = "\n".join(res.schema_sql)
        assert "ADD COLUMN" in sql
        assert "`email`" in sql

    def test_drop_column_produces_alter_drop(self):
        old = {"users": _snap("users", USERS_DDL, [], ["id"])}
        new = {"users": _snap("users", USERS_DDL_REMOVED_COL, [], ["id"])}
        res = diff_snapshots(old, new)
        sql = "\n".join(res.schema_sql)
        assert "DROP COLUMN" in sql
        assert "`name`" in sql

    def test_column_type_change_emits_warning_and_modify(self):
        old_ddl = "CREATE TABLE `t` (`id` INT PRIMARY KEY, `v` VARCHAR(10));"
        new_ddl = "CREATE TABLE `t` (`id` INT PRIMARY KEY, `v` TEXT);"
        old = {"t": _snap("t", old_ddl, [], ["id"])}
        new = {"t": _snap("t", new_ddl, [], ["id"])}
        res = diff_snapshots(old, new)
        assert any("MODIFY COLUMN" in s for s in res.schema_sql)
        assert len(res.warnings) > 0

    def test_schema_ordering_create_before_drop(self):
        """CREATEs and ALTERs must come before DROPs in schema_sql."""
        old = {"a": _snap("a", "CREATE TABLE `a` (`id` INT PRIMARY KEY);", [], ["id"])}
        new = {"b": _snap("b", "CREATE TABLE `b` (`id` INT PRIMARY KEY);", [], ["id"])}
        res = diff_snapshots(old, new)
        create_idx = next(i for i, s in enumerate(res.schema_sql) if "CREATE" in s)
        drop_idx = next(i for i, s in enumerate(res.schema_sql) if "DROP" in s)
        assert create_idx < drop_idx

    def test_identical_snapshot_no_schema_sql(self):
        snap = {"users": _snap("users", USERS_DDL, [], ["id"])}
        res = diff_snapshots(snap, snap)
        assert res.schema_sql == []


# ── DiffResult structure ──────────────────────────────────────────────────────

class TestDiffResult:
    def test_result_is_dataclass(self):
        res = diff_snapshots({}, {})
        assert isinstance(res, DiffResult)
        assert isinstance(res.schema_sql, list)
        assert isinstance(res.data_sql, list)
        assert isinstance(res.warnings, list)


# ── load_snapshot_from_row ────────────────────────────────────────────────────

class TestLoadSnapshotFromRow:
    def test_roundtrip(self):
        import json

        ddl = json.dumps(
            {
                "raw_ddl": USERS_DDL,
                "columns": [{"name": "id", "type": "int", "nullable": False, "key": "PRI", "default": None, "extra": ""}],
                "pk_columns": ["id"],
            }
        )
        rows = json.dumps([{"id": 1, "name": "alice"}])
        ts = load_snapshot_from_row(ddl, rows, "users")
        assert ts.table_name == "users"
        assert ts.pk_columns == ["id"]
        assert ts.rows_json == [{"id": 1, "name": "alice"}]

    def test_pk_fallback_from_columns(self):
        import json

        ddl = json.dumps(
            {
                "raw_ddl": USERS_DDL,
                "columns": [
                    {"name": "id", "type": "int", "nullable": False, "key": "PRI", "default": None, "extra": ""},
                    {"name": "name", "type": "varchar(50)", "nullable": True, "key": "", "default": None, "extra": ""},
                ],
                # No 'pk_columns' key – should derive from columns
            }
        )
        rows = json.dumps([])
        ts = load_snapshot_from_row(ddl, rows, "users")
        assert "id" in ts.pk_columns
