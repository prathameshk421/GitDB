"""Unit tests for engine/hash.py – no live DB required."""
from __future__ import annotations

import hashlib

import pytest

from engine.hash import compute_commit_hash
from engine.snapshot import TableSnapshot


# ── Helper ────────────────────────────────────────────────────────────────────

def _snap(table: str = "t") -> dict[str, TableSnapshot]:
    return {
        table: TableSnapshot(
            table_name=table,
            ddl_json={"raw_ddl": f"CREATE TABLE `{table}` (`id` INT PRIMARY KEY);"},
            rows_json=[{"id": 1}, {"id": 2}],
            row_count=2,
            pk_columns=["id"],
        )
    }


# ── Determinism ───────────────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_input_same_hash(self):
        snap = _snap()
        assert compute_commit_hash(snap, None) == compute_commit_hash(snap, None)

    def test_same_input_with_parent_same_hash(self):
        snap = _snap()
        parent = "abc" * 21  # 63-char fake hash
        assert compute_commit_hash(snap, parent) == compute_commit_hash(snap, parent)

    def test_order_insensitive_to_row_insertion_order_when_identical(self):
        """Same rows, same order → same hash."""
        snap_a = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
                rows_json=[{"id": 1}, {"id": 2}],
                row_count=2,
                pk_columns=["id"],
            )
        }
        snap_b = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
                rows_json=[{"id": 1}, {"id": 2}],
                row_count=2,
                pk_columns=["id"],
            )
        }
        assert compute_commit_hash(snap_a, None) == compute_commit_hash(snap_b, None)


# ── Sensitivity ───────────────────────────────────────────────────────────────

class TestSensitivity:
    def test_different_parent_different_hash(self):
        snap = _snap()
        h1 = compute_commit_hash(snap, None)
        h2 = compute_commit_hash(snap, "someParentHash")
        assert h1 != h2

    def test_different_rows_different_hash(self):
        snap_a = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
                rows_json=[{"id": 1}],
                row_count=1,
                pk_columns=["id"],
            )
        }
        snap_b = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
                rows_json=[{"id": 2}],
                row_count=1,
                pk_columns=["id"],
            )
        }
        assert compute_commit_hash(snap_a, None) != compute_commit_hash(snap_b, None)

    def test_different_schema_different_hash(self):
        snap_a = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
                rows_json=[],
                row_count=0,
                pk_columns=["id"],
            )
        }
        snap_b = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY, `name` VARCHAR(50));"},
                rows_json=[],
                row_count=0,
                pk_columns=["id"],
            )
        }
        assert compute_commit_hash(snap_a, None) != compute_commit_hash(snap_b, None)

    def test_different_table_name_different_hash(self):
        assert compute_commit_hash(_snap("a"), None) != compute_commit_hash(_snap("b"), None)

    def test_empty_snapshot_hash(self):
        """Root commit with empty snapshot must still produce a 64-char hex string."""
        h = compute_commit_hash({}, None)
        assert len(h) == 64
        int(h, 16)  # must parse as hex without exception


# ── Format ────────────────────────────────────────────────────────────────────

class TestOutputFormat:
    def test_returns_64_char_hex(self):
        h = compute_commit_hash(_snap(), None)
        assert len(h) == 64
        int(h, 16)

    def test_is_sha256(self):
        """Spot-check: empty snapshot with no parent matches raw SHA-256."""
        from engine.snapshot import snapshot_to_json
        snap = {}
        json_bytes = snapshot_to_json(snap).encode("utf-8")
        payload = json_bytes + b""
        expected = hashlib.sha256(payload).hexdigest()
        assert compute_commit_hash(snap, None) == expected


# ── Merkle chain propagation ──────────────────────────────────────────────────

class TestMerkleChain:
    def test_three_commit_chain_each_unique(self):
        snap = _snap()
        h0 = compute_commit_hash(snap, None)
        h1 = compute_commit_hash(snap, h0)
        h2 = compute_commit_hash(snap, h1)
        assert len({h0, h1, h2}) == 3  # all distinct

    def test_modifying_ancestor_changes_child_hash(self):
        snap_a = _snap()
        snap_b = {
            "t": TableSnapshot(
                table_name="t",
                ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
                rows_json=[{"id": 999}],
                row_count=1,
                pk_columns=["id"],
            )
        }
        h0_original = compute_commit_hash(snap_a, None)
        h0_modified = compute_commit_hash(snap_b, None)
        # Child built on different parent → different hash
        child_original = compute_commit_hash(snap_a, h0_original)
        child_modified = compute_commit_hash(snap_a, h0_modified)
        assert child_original != child_modified
