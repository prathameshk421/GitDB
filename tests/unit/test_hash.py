from engine.hash import compute_commit_hash
from engine.snapshot import TableSnapshot


def test_compute_commit_hash_deterministic():
    snap = {
        "t": TableSnapshot(
            table_name="t",
            ddl_json={"raw_ddl": "CREATE TABLE t (id INT PRIMARY KEY);"},
            rows_json=[{"id": 1}, {"id": 2}],
            row_count=2,
            pk_columns=["id"],
        )
    }
    h1 = compute_commit_hash(snap, None)
    h2 = compute_commit_hash(snap, None)
    assert h1 == h2


def test_compute_commit_hash_parent_affects_hash():
    snap = {
        "t": TableSnapshot(
            table_name="t",
            ddl_json={"raw_ddl": "CREATE TABLE t (id INT PRIMARY KEY);"},
            rows_json=[{"id": 1}],
            row_count=1,
            pk_columns=["id"],
        )
    }
    h1 = compute_commit_hash(snap, None)
    h2 = compute_commit_hash(snap, "parent")
    assert h1 != h2

