"""Unit tests for engine/snapshot.py – uses mocked cursor, no live DB."""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

from engine.snapshot import (
    TableSnapshot,
    capture_snapshot,
    snapshot_to_json,
)
from engine.errors import SnapshotRowLimitError


# ── Helper: build a mock cursor ───────────────────────────────────────────────

def _make_cursor(tables, pk_cols_map, col_meta_map, row_counts_map, rows_map, raw_ddl_map):
    """
    Returns a mock cursor whose fetchall/fetchone are rigged to respond in
    the order that capture_snapshot calls them.
    """
    cursor = MagicMock()
    calls = []

    def execute_side_effect(sql, params=None):
        calls.append((sql.strip()[:60], params))

    cursor.execute.side_effect = execute_side_effect

    # Sequence of values returned by fetchall / fetchone.
    # capture_snapshot calls per-table:
    #   _get_tables        → fetchall returning [(name,), ...]
    #   _get_pk_columns    → fetchall returning [(col,), ...]
    #   _get_columns       → fetchall returning [(name,type,null,def,key,extra), ...]
    #   _get_raw_ddl       → fetchone returning (name, ddl_str)
    #   _count_rows        → fetchone returning (count,)
    #   _fetch_rows        → fetchall returning [{...}, ...]

    fetchall_returns = [[(t,) for t in tables]]  # get_tables
    fetchone_returns = []

    for t in tables:
        pk = pk_cols_map.get(t, ["id"])
        fetchall_returns.append([(c,) for c in pk])  # get_pk_columns
        cols = col_meta_map.get(t, [])
        fetchall_returns.append(cols)  # get_columns
        fetchone_returns.append((t, raw_ddl_map.get(t, f"CREATE TABLE `{t}` (`id` INT PRIMARY KEY);")))  # get_raw_ddl
        fetchone_returns.append((row_counts_map.get(t, 0),))  # count_rows
        fetchall_returns.append([])  # _fetchall_dict for rows – populate below if needed

    # Patch _fetch_rows separately – it calls fetchall with column description
    # We monkey-patch at the module level using patch instead.

    fetchall_iter = iter(fetchall_returns)
    fetchone_iter = iter(fetchone_returns)

    cursor.fetchall.side_effect = lambda: next(fetchall_iter)
    cursor.fetchone.side_effect = lambda: next(fetchone_iter)
    cursor.description = []  # used by _fetchall_dict

    return cursor


# ── TableSnapshot dataclass ───────────────────────────────────────────────────

class TestTableSnapshotDataclass:
    def test_fields(self):
        ts = TableSnapshot(
            table_name="t",
            ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
            rows_json=[{"id": 1}],
            row_count=1,
            pk_columns=["id"],
        )
        assert ts.table_name == "t"
        assert ts.row_count == 1
        assert ts.pk_columns == ["id"]

    def test_empty_pk_columns(self):
        ts = TableSnapshot(
            table_name="nopk",
            ddl_json={"raw_ddl": "CREATE TABLE `nopk` (`name` VARCHAR(50));"},
            rows_json=[],
            row_count=0,
            pk_columns=[],
        )
        assert ts.pk_columns == []


# ── snapshot_to_json ──────────────────────────────────────────────────────────

class TestSnapshotToJson:
    def test_is_valid_json(self):
        import json
        snap = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
                rows_json=[{"id": 1}],
                row_count=1,
                pk_columns=["id"],
            )
        }
        j = snapshot_to_json(snap)
        obj = json.loads(j)
        assert "t" in obj

    def test_sorted_keys_deterministic(self):
        snap_ab = {
            "a": TableSnapshot(table_name="a", ddl_json={}, rows_json=[], row_count=0, pk_columns=[]),
            "b": TableSnapshot(table_name="b", ddl_json={}, rows_json=[], row_count=0, pk_columns=[]),
        }
        snap_ba = {
            "b": TableSnapshot(table_name="b", ddl_json={}, rows_json=[], row_count=0, pk_columns=[]),
            "a": TableSnapshot(table_name="a", ddl_json={}, rows_json=[], row_count=0, pk_columns=[]),
        }
        assert snapshot_to_json(snap_ab) == snapshot_to_json(snap_ba)

    def test_empty_snapshot(self):
        import json
        j = snapshot_to_json({})
        assert json.loads(j) == {}


# ── Row limit guard ───────────────────────────────────────────────────────────

class TestRowLimitError:
    def test_raises_on_excess_rows(self, monkeypatch):
        """capture_snapshot must raise SnapshotRowLimitError before fetching rows."""
        from engine import snapshot as snap_mod

        # Patch internal helpers so we control the number of rows
        monkeypatch.setattr(snap_mod, "_get_tables", lambda cur, db: ["big_table"])
        monkeypatch.setattr(snap_mod, "_get_pk_columns", lambda cur, db, t: ["id"])
        monkeypatch.setattr(snap_mod, "_get_columns", lambda cur, db, t: [])
        monkeypatch.setattr(snap_mod, "_get_raw_ddl", lambda cur, t: "CREATE TABLE `big_table` (`id` INT PRIMARY KEY);")
        monkeypatch.setattr(snap_mod, "_count_rows", lambda cur, t: 10_001)
        # _fetch_rows should never be called
        fetch_called = []
        monkeypatch.setattr(snap_mod, "_fetch_rows", lambda *a: fetch_called.append(True) or [])

        conn = MagicMock()
        conn.cursor.return_value = MagicMock()

        with pytest.raises(SnapshotRowLimitError, match="10001"):
            capture_snapshot(conn, "testdb", row_limit=10_000)

        assert fetch_called == [], "_fetch_rows must not be called when limit exceeded"

    def test_does_not_raise_at_limit(self, monkeypatch):
        from engine import snapshot as snap_mod

        monkeypatch.setattr(snap_mod, "_get_tables", lambda cur, db: ["t"])
        monkeypatch.setattr(snap_mod, "_get_pk_columns", lambda cur, db, t: ["id"])
        monkeypatch.setattr(snap_mod, "_get_columns", lambda cur, db, t: [])
        monkeypatch.setattr(snap_mod, "_get_raw_ddl", lambda cur, t: "CREATE TABLE `t` (`id` INT PRIMARY KEY);")
        monkeypatch.setattr(snap_mod, "_count_rows", lambda cur, t: 10_000)
        monkeypatch.setattr(snap_mod, "_fetch_rows", lambda cur, t, pk: [{"id": i} for i in range(10_000)])

        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        snap = capture_snapshot(conn, "testdb", row_limit=10_000)
        assert snap["t"].row_count == 10_000


# ── No-PK table skipping ──────────────────────────────────────────────────────

class TestNoPkSkipping:
    def test_no_pk_table_captured_no_rows(self, monkeypatch):
        from engine import snapshot as snap_mod

        monkeypatch.setattr(snap_mod, "_get_tables", lambda cur, db: ["nopk"])
        monkeypatch.setattr(snap_mod, "_get_pk_columns", lambda cur, db, t: [])  # empty PK
        monkeypatch.setattr(snap_mod, "_get_columns", lambda cur, db, t: [])
        monkeypatch.setattr(snap_mod, "_get_raw_ddl", lambda cur, t: "CREATE TABLE `nopk` (`x` INT);")
        count_called = []
        monkeypatch.setattr(snap_mod, "_count_rows", lambda cur, t: count_called.append(1) or 0)
        monkeypatch.setattr(snap_mod, "_fetch_rows", lambda *a: [])

        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        snap = capture_snapshot(conn, "testdb")

        assert "nopk" in snap
        assert snap["nopk"].rows_json == []
        assert snap["nopk"].row_count == 0
        # _count_rows must NOT be called for no-PK tables
        assert count_called == []

    def test_pk_table_rows_fetched(self, monkeypatch):
        from engine import snapshot as snap_mod

        monkeypatch.setattr(snap_mod, "_get_tables", lambda cur, db: ["t"])
        monkeypatch.setattr(snap_mod, "_get_pk_columns", lambda cur, db, t: ["id"])
        monkeypatch.setattr(snap_mod, "_get_columns", lambda cur, db, t: [])
        monkeypatch.setattr(snap_mod, "_get_raw_ddl", lambda cur, t: "CREATE TABLE `t` (`id` INT PRIMARY KEY);")
        monkeypatch.setattr(snap_mod, "_count_rows", lambda cur, t: 2)
        monkeypatch.setattr(snap_mod, "_fetch_rows", lambda cur, t, pk: [{"id": 1}, {"id": 2}])

        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        snap = capture_snapshot(conn, "testdb")

        assert snap["t"].row_count == 2
        assert snap["t"].rows_json == [{"id": 1}, {"id": 2}]
