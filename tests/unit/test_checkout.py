"""Unit tests for engine/checkout.py – no live DB required."""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from engine.checkout import apply_checkout, write_recovery_file, restore_schema_from_snapshot
from engine.diff import DiffResult
from engine.errors import CheckoutDataError, CheckoutSchemaError
from engine.snapshot import TableSnapshot


# ── Helpers ───────────────────────────────────────────────────────────────────

def _snap(table: str = "t") -> dict[str, TableSnapshot]:
    return {
        table: TableSnapshot(
            table_name=table,
            ddl_json={"raw_ddl": f"CREATE TABLE `{table}` (`id` INT PRIMARY KEY);"},
            rows_json=[{"id": 1}],
            row_count=1,
            pk_columns=["id"],
        )
    }


def _empty_diff():
    return DiffResult(schema_sql=[], data_sql=[], warnings=[])


def _mock_conn():
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    return conn


# ── write_recovery_file ───────────────────────────────────────────────────────

class TestWriteRecoveryFile:
    def test_creates_file_with_raw_ddl(self, tmp_path):
        snap = _snap()
        path = str(tmp_path / "recovery.sql")
        result = write_recovery_file(snap, path=path)
        content = Path(result).read_text()
        assert "CREATE TABLE" in content
        assert "GitDB recovery file" in content

    def test_creates_parent_directories(self, tmp_path):
        snap = _snap()
        path = str(tmp_path / "nested" / "dir" / "recovery.sql")
        write_recovery_file(snap, path=path)
        assert Path(path).exists()

    def test_returns_path_string(self, tmp_path):
        snap = _snap()
        path = str(tmp_path / "r.sql")
        result = write_recovery_file(snap, path=path)
        assert isinstance(result, str)
        assert result == path

    def test_no_pk_table_still_written(self, tmp_path):
        snap = {
            "nopk": TableSnapshot(
                table_name="nopk",
                ddl_json={"raw_ddl": "CREATE TABLE `nopk` (`x` INT);"},
                rows_json=[],
                row_count=0,
                pk_columns=[],
            )
        }
        path = str(tmp_path / "r.sql")
        write_recovery_file(snap, path=path)
        assert "CREATE TABLE" in Path(path).read_text()


# ── apply_checkout – happy path ───────────────────────────────────────────────

class TestApplyCheckout:
    def test_no_diff_no_cursor_calls(self):
        conn = _mock_conn()
        cursor = conn.cursor.return_value
        result = apply_checkout(conn, diff=_empty_diff(), old_snapshot=_snap())
        assert result is None
        # START TRANSACTION / FK_CHECKS / COMMIT are still called even with empty diff
        executed = [c.args[0] for c in cursor.execute.call_args_list]
        assert "START TRANSACTION" in executed

    def test_schema_statements_executed_in_order(self):
        conn = _mock_conn()
        cursor = conn.cursor.return_value
        diff = DiffResult(
            schema_sql=["ALTER TABLE `t` ADD COLUMN `v` INT;", "ALTER TABLE `t` DROP COLUMN `old`;"],
            data_sql=[],
            warnings=[],
        )
        apply_checkout(conn, diff=diff, old_snapshot=_snap())
        executed = [c.args[0] for c in cursor.execute.call_args_list]
        assert "ALTER TABLE `t` ADD COLUMN `v` INT;" in executed
        assert executed.index("ALTER TABLE `t` ADD COLUMN `v` INT;") < executed.index("ALTER TABLE `t` DROP COLUMN `old`;")

    def test_data_statements_wrapped_in_transaction(self):
        conn = _mock_conn()
        cursor = conn.cursor.return_value
        diff = DiffResult(
            schema_sql=[],
            data_sql=["INSERT INTO `t` (`id`) VALUES (99);"],
            warnings=[],
        )
        apply_checkout(conn, diff=diff, old_snapshot=_snap())
        executed = [c.args[0] for c in cursor.execute.call_args_list]
        assert "START TRANSACTION" in executed
        assert "INSERT INTO `t` (`id`) VALUES (99);" in executed
        assert "SET FOREIGN_KEY_CHECKS = 0" in executed
        assert "SET FOREIGN_KEY_CHECKS = 1" in executed
        conn.commit.assert_called_once()

    def test_returns_none_on_success(self):
        result = apply_checkout(_mock_conn(), diff=_empty_diff(), old_snapshot=_snap())
        assert result is None


# ── apply_checkout – schema failure ──────────────────────────────────────────

class TestSchemaFailure:
    def test_raises_checkout_schema_error(self, tmp_path):
        conn = _mock_conn()
        cursor = conn.cursor.return_value
        # Use a function so that calls after the first one (restore_schema_from_snapshot)
        # don't raise StopIteration.
        first_call = [True]

        def _side_effect(sql, *a, **kw):
            if first_call[0]:
                first_call[0] = False
                raise Exception("DDL boom")
            # subsequent calls (restore path) succeed silently

        cursor.execute.side_effect = _side_effect
        cursor.fetchone.return_value = ("testdb",)
        cursor.fetchall.return_value = []

        diff = DiffResult(schema_sql=["BAD DDL;"], data_sql=[], warnings=[])
        recovery_path = str(tmp_path / "recovery.sql")

        with pytest.raises(CheckoutSchemaError, match="DDL boom"):
            apply_checkout(conn, diff=diff, old_snapshot=_snap(), recovery_file_path=recovery_path)

    def test_recovery_file_written_on_schema_failure(self, tmp_path):
        conn = _mock_conn()
        cursor = conn.cursor.return_value

        first_call = [True]

        def side_effect(sql, *a, **kw):
            if first_call[0]:
                first_call[0] = False
                raise Exception("DDL boom")
            # restore_schema_from_snapshot calls succeed silently

        cursor.execute.side_effect = side_effect
        cursor.fetchone.return_value = ("testdb",)
        cursor.fetchall.return_value = []

        diff = DiffResult(schema_sql=["BAD DDL;"], data_sql=[], warnings=[])
        recovery_path = str(tmp_path / "recovery.sql")

        with pytest.raises(CheckoutSchemaError):
            apply_checkout(conn, diff=diff, old_snapshot=_snap(), recovery_file_path=recovery_path)

        assert Path(recovery_path).exists(), "Recovery file must be written on schema failure"

    def test_data_phase_not_executed_after_schema_failure(self, tmp_path):
        conn = _mock_conn()
        cursor = conn.cursor.return_value
        cursor.fetchone.return_value = ("testdb",)
        cursor.fetchall.return_value = []

        executed = []

        def side_effect(sql, *a, **kw):
            executed.append(sql)
            if sql == "FAIL;":
                raise Exception("DDL boom")

        cursor.execute.side_effect = side_effect

        diff = DiffResult(
            schema_sql=["FAIL;"],
            data_sql=["INSERT INTO `t` VALUES (1);"],
            warnings=[],
        )

        with pytest.raises(CheckoutSchemaError):
            apply_checkout(conn, diff=diff, old_snapshot=_snap(), recovery_file_path=str(tmp_path / "r.sql"))

        assert "INSERT INTO `t` VALUES (1);" not in executed


# ── apply_checkout – data failure ────────────────────────────────────────────

class TestDataFailure:
    def test_raises_checkout_data_error(self):
        conn = _mock_conn()
        cursor = conn.cursor.return_value

        def side_effect(sql, *a, **kw):
            if sql.startswith("INSERT"):
                raise Exception("FK violation")

        cursor.execute.side_effect = side_effect

        diff = DiffResult(
            schema_sql=[],
            data_sql=["INSERT INTO `t` VALUES (1);"],
            warnings=[],
        )

        with pytest.raises(CheckoutDataError, match="FK violation"):
            apply_checkout(conn, diff=diff, old_snapshot=_snap())

    def test_rollback_called_on_data_failure(self):
        conn = _mock_conn()
        cursor = conn.cursor.return_value

        def side_effect(sql, *a, **kw):
            if sql.startswith("INSERT"):
                raise Exception("boom")

        cursor.execute.side_effect = side_effect

        diff = DiffResult(schema_sql=[], data_sql=["INSERT INTO `t` VALUES (1);"], warnings=[])

        with pytest.raises(CheckoutDataError):
            apply_checkout(conn, diff=diff, old_snapshot=_snap())

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
