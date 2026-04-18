"""
Integration tests – require a live MySQL 8 instance.

Run with:
    GITDB_TEST_MYSQL_URL=mysql://root:secret@localhost:3306 pytest tests/integration/ -v

Tests are automatically skipped when GITDB_TEST_MYSQL_URL is not set.

Covered scenarios from the Implementation Plan (Phase 10):
  1. Full round-trip: commit A → modify DB → commit B → checkout A → assert DB == A
  2. Row limit: seed 10,001 rows → assert SnapshotRowLimitError raised
  3. Schema-only checkout: add column → commit → drop column → commit → checkout prior → assert column restored
  4. No-PK table skipping: table without PK is captured (DDL only) without raising
  5. FK ordering: snapshot a DB with FK-linked tables → checkout → assert no FK violations
"""
from __future__ import annotations

import pytest

from engine.snapshot import capture_snapshot
from engine.diff import diff_snapshots
from engine.checkout import apply_checkout
from engine.errors import SnapshotRowLimitError
from engine.hash import compute_commit_hash


# ── Utility helpers ───────────────────────────────────────────────────────────

def _exec(conn, sql: str, params=None) -> None:
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()


def _fetchall(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


# ── 1. Full round-trip ────────────────────────────────────────────────────────

@pytest.mark.integration
class TestFullRoundTrip:
    def test_checkout_restores_previous_state(self, test_db):
        conn, db_name = test_db

        # State A: single table with 2 rows
        _exec(conn, "CREATE TABLE `items` (`id` INT PRIMARY KEY, `val` VARCHAR(50))")
        _exec(conn, "INSERT INTO `items` VALUES (1, 'alpha'), (2, 'beta')")

        snap_a = capture_snapshot(conn, db_name)
        hash_a = compute_commit_hash(snap_a, None)

        # State B: update a row, add a row
        _exec(conn, "UPDATE `items` SET `val` = 'alpha_v2' WHERE `id` = 1")
        _exec(conn, "INSERT INTO `items` VALUES (3, 'gamma')")

        snap_b = capture_snapshot(conn, db_name)
        hash_b = compute_commit_hash(snap_b, hash_a)

        # Checkout back to state A
        diff = diff_snapshots(snap_b, snap_a)
        apply_checkout(conn, diff=diff, old_snapshot=snap_b)
        conn.commit()

        # DB should now match state A
        rows = _fetchall(conn, "SELECT `id`, `val` FROM `items` ORDER BY `id`")
        assert rows == [(1, "alpha"), (2, "beta")], f"Unexpected rows after checkout: {rows}"

    def test_checkout_to_empty_db_drops_all_tables(self, test_db):
        conn, db_name = test_db

        snap_empty = capture_snapshot(conn, db_name)

        _exec(conn, "CREATE TABLE `t1` (`id` INT PRIMARY KEY)")
        _exec(conn, "INSERT INTO `t1` VALUES (1)")
        snap_with_data = capture_snapshot(conn, db_name)

        diff = diff_snapshots(snap_with_data, snap_empty)
        apply_checkout(conn, diff=diff, old_snapshot=snap_with_data)
        conn.commit()

        snap_after = capture_snapshot(conn, db_name)
        assert snap_after == {}, "DB should be empty after checkout to empty snapshot"


# ── 2. Row limit ──────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestRowLimit:
    def test_raises_before_fetching_any_rows(self, test_db):
        conn, db_name = test_db

        _exec(conn, "CREATE TABLE `big` (`id` INT PRIMARY KEY)")
        # Insert 10,001 rows via a numbers trick
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO `big` (`id`)
            WITH RECURSIVE n(i) AS (
                SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10001
            )
            SELECT i FROM n
            """
        )
        conn.commit()

        with pytest.raises(SnapshotRowLimitError, match="10001"):
            capture_snapshot(conn, db_name, row_limit=10_000)

    def test_exactly_at_limit_succeeds(self, test_db):
        conn, db_name = test_db

        _exec(conn, "CREATE TABLE `exact` (`id` INT PRIMARY KEY)")
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO `exact` (`id`)
            WITH RECURSIVE n(i) AS (
                SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10000
            )
            SELECT i FROM n
            """
        )
        conn.commit()

        snap = capture_snapshot(conn, db_name, row_limit=10_000)
        assert snap["exact"].row_count == 10_000


# ── 3. Schema-only checkout ───────────────────────────────────────────────────

@pytest.mark.integration
class TestSchemaOnlyCheckout:
    def test_added_column_restored_after_checkout(self, test_db):
        conn, db_name = test_db

        _exec(conn, "CREATE TABLE `t` (`id` INT PRIMARY KEY, `name` VARCHAR(50))")
        snap_before_add = capture_snapshot(conn, db_name)

        _exec(conn, "ALTER TABLE `t` ADD COLUMN `email` VARCHAR(100)")
        snap_after_add = capture_snapshot(conn, db_name)

        # Checkout to before the column was added
        diff = diff_snapshots(snap_after_add, snap_before_add)
        apply_checkout(conn, diff=diff, old_snapshot=snap_after_add)
        conn.commit()

        # Check the column is gone
        cur = conn.cursor()
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'T' AND COLUMN_NAME = 'email'",
            (db_name,),
        )
        assert cur.fetchone() is None, "Column `email` should have been dropped"

    def test_dropped_column_restored_after_checkout(self, test_db):
        conn, db_name = test_db

        _exec(conn, "CREATE TABLE `t` (`id` INT PRIMARY KEY, `extra` VARCHAR(50))")
        snap_with_col = capture_snapshot(conn, db_name)

        _exec(conn, "ALTER TABLE `t` DROP COLUMN `extra`")
        snap_without_col = capture_snapshot(conn, db_name)

        diff = diff_snapshots(snap_without_col, snap_with_col)
        apply_checkout(conn, diff=diff, old_snapshot=snap_without_col)
        conn.commit()

        # Column should be back
        cur = conn.cursor()
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'T' AND COLUMN_NAME = 'extra'",
            (db_name,),
        )
        assert cur.fetchone() is not None, "Column `extra` should have been restored"


# ── 4. No-PK table skipping ───────────────────────────────────────────────────

@pytest.mark.integration
class TestNoPkTableSkipping:
    def test_snapshot_does_not_raise_for_no_pk_table(self, test_db):
        conn, db_name = test_db

        _exec(conn, "CREATE TABLE `nopk` (`name` VARCHAR(50), `value` INT)")
        _exec(conn, "INSERT INTO `nopk` VALUES ('x', 1)")

        snap = capture_snapshot(conn, db_name)
        assert "nopk" in snap
        assert snap["nopk"].rows_json == [], "No-PK table must have empty rows_json"
        assert snap["nopk"].pk_columns == []


# ── 5. FK ordering ────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestFkOrdering:
    def test_checkout_respects_fk_with_fk_checks_off(self, test_db):
        """
        Create parent → child FK relation. Commit state A with data.
        Delete all data. Commit state B.
        Checkout A. Assert data restored without FK violation.
        """
        conn, db_name = test_db

        _exec(conn, "CREATE TABLE `parent` (`id` INT PRIMARY KEY)")
        _exec(conn,
            "CREATE TABLE `child` (`id` INT PRIMARY KEY, `parent_id` INT, "
            "CONSTRAINT fk_ch FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`))"
        )
        _exec(conn, "INSERT INTO `parent` VALUES (1), (2)")
        _exec(conn, "INSERT INTO `child` VALUES (10, 1), (20, 2)")

        snap_a = capture_snapshot(conn, db_name)

        _exec(conn, "DELETE FROM `child`")
        _exec(conn, "DELETE FROM `parent`")

        snap_b = capture_snapshot(conn, db_name)

        # checkout back to A; if FK_CHECKS aren't toggled this may fail on INSERT order
        diff = diff_snapshots(snap_b, snap_a)
        apply_checkout(conn, diff=diff, old_snapshot=snap_b)
        conn.commit()

        parent_rows = _fetchall(conn, "SELECT `id` FROM `parent` ORDER BY `id`")
        child_rows = _fetchall(conn, "SELECT `id`, `parent_id` FROM `child` ORDER BY `id`")
        assert parent_rows == [(1,), (2,)]
        assert child_rows == [(10, 1), (20, 2)]
