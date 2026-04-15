from engine.diff import diff_snapshots
from engine.snapshot import TableSnapshot


def test_data_diff_insert_update_delete():
    old = {
        "users": TableSnapshot(
            table_name="users",
            ddl_json={"raw_ddl": "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(10));"},
            rows_json=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            row_count=2,
            pk_columns=["id"],
        )
    }
    new = {
        "users": TableSnapshot(
            table_name="users",
            ddl_json={"raw_ddl": "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(10));"},
            rows_json=[{"id": 1, "name": "a2"}, {"id": 3, "name": "c"}],
            row_count=2,
            pk_columns=["id"],
        )
    }
    res = diff_snapshots(old, new)
    sql = "\n".join(res.data_sql)
    assert "INSERT INTO `users`" in sql
    assert "UPDATE `users`" in sql
    assert "DELETE FROM `users`" in sql

