"""
API tests for api/app.py using Flask's test client.

All MySQL calls are monkeypatched so no live DB is required.
The key insight: functions are patched at the *api.app* module level (where Flask
route handlers import them), not at engine.db, so the patches actually take effect
inside the running request.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from engine.snapshot import TableSnapshot


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def fake_cfg():
    cfg = MagicMock()
    cfg.repo_id = 1
    cfg.host = "localhost"
    cfg.port = 3306
    cfg.db_user = "root"
    cfg.db_name = "testdb"
    return cfg


@pytest.fixture()
def app(fake_cfg, monkeypatch):
    """Create a test Flask app with all DB calls patched at api.app namespace."""
    import api.app as app_mod

    monkeypatch.setattr(app_mod, "load_repo_config", lambda: fake_cfg)
    monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: MagicMock())
    monkeypatch.setattr(app_mod, "connect_target_db", lambda cfg: MagicMock())

    from api.app import create_app
    flask_app = create_app()
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


# ── GET /commits ──────────────────────────────────────────────────────────────

class TestGetCommits:
    def test_returns_json_list(self, client, fake_cfg, monkeypatch):
        import api.app as app_mod

        meta = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("a" * 64, None, "initial commit", "2026-01-01 00:00:00", "alice", "Alice Smith"),
        ]
        meta.cursor.return_value = cur
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)

        resp = client.get("/commits")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert isinstance(data, list)
        assert data[0]["message"] == "initial commit"

    def test_empty_returns_empty_list(self, client, monkeypatch):
        import api.app as app_mod

        meta = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = []
        meta.cursor.return_value = cur
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)

        resp = client.get("/commits")
        assert resp.status_code == 200
        assert json.loads(resp.data) == []


# ── GET /diff/<h1>/<h2> ───────────────────────────────────────────────────────

class TestGetDiff:
    def test_diff_get_returns_structure(self, client, monkeypatch):
        import api.app as app_mod

        meta = MagicMock()
        cur = MagicMock()
        empty_ddl = json.dumps({"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);", "columns": []})
        empty_rows = json.dumps([])
        cur.fetchall.side_effect = [
            [("t", empty_ddl, empty_rows)],  # hash1 snapshots
            [("t", empty_ddl, empty_rows)],  # hash2 snapshots
        ]
        meta.cursor.return_value = cur
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)

        resp = client.get("/diff/hash1/hash2")
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert "schema_sql" in body
        assert "data_sql" in body
        assert "warnings" in body

    def test_diff_post_also_works(self, client, monkeypatch):
        import api.app as app_mod

        meta = MagicMock()
        cur = MagicMock()
        cur.fetchall.side_effect = [[], []]
        meta.cursor.return_value = cur
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)

        resp = client.post("/diff/h1/h2")
        assert resp.status_code == 200


# ── GET /snapshot/<hash> ──────────────────────────────────────────────────────

class TestGetSnapshot:
    def test_returns_table_list(self, client, monkeypatch):
        import api.app as app_mod

        meta = MagicMock()
        cur = MagicMock()
        ddl_blob = json.dumps({"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);", "columns": []})
        cur.fetchall.return_value = [("t", ddl_blob, 5)]
        meta.cursor.return_value = cur
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)

        resp = client.get("/snapshot/somehash")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert len(data) == 1
        assert data[0]["table_name"] == "t"
        assert data[0]["row_count"] == 5


# ── GET /status ───────────────────────────────────────────────────────────────

class TestGetStatus:
    def test_no_head_returns_400(self, client, monkeypatch):
        import api.app as app_mod

        meta = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = (None,)
        meta.cursor.return_value = cur
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)
        monkeypatch.setattr(app_mod, "connect_target_db", lambda cfg: MagicMock())

        resp = client.get("/status")
        assert resp.status_code == 400
        assert "error" in json.loads(resp.data)

    def test_clean_status(self, client, monkeypatch):
        import api.app as app_mod

        meta = MagicMock()
        target = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = ("a" * 64,)

        snap_ddl = json.dumps({"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);", "columns": []})
        snap_rows = json.dumps([{"id": 1}])
        cur.fetchall.return_value = [("t", snap_ddl, snap_rows)]
        meta.cursor.return_value = cur

        live_ts = TableSnapshot(
            table_name="t",
            ddl_json={"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);"},
            rows_json=[{"id": 1}],
            row_count=1,
            pk_columns=["id"],
        )
        # Patch at the api.app namespace where the route function sees it
        monkeypatch.setattr(app_mod, "capture_snapshot", lambda conn, db_name, **kw: {"t": live_ts})
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)
        monkeypatch.setattr(app_mod, "connect_target_db", lambda cfg: target)

        resp = client.get("/status")
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body["added_tables"] == []
        assert body["dropped_tables"] == []
        assert body["modified_tables"] == []


# ── POST /checkout/<hash> ─────────────────────────────────────────────────────

class TestPostCheckout:
    def _setup_meta(self, monkeypatch, head_hash: str | None):
        import api.app as app_mod

        meta = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = (head_hash,)

        snap_ddl = json.dumps({"raw_ddl": "CREATE TABLE `t` (`id` INT PRIMARY KEY);", "columns": []})
        snap_rows = json.dumps([{"id": 1}])
        cur.fetchall.side_effect = [
            [("t", snap_ddl, snap_rows)],  # old_snap
            [("t", snap_ddl, snap_rows)],  # new_snap
        ]
        meta.cursor.return_value = cur
        monkeypatch.setattr(app_mod, "connect_meta_db", lambda cfg: meta)
        monkeypatch.setattr(app_mod, "connect_target_db", lambda cfg: MagicMock())
        return meta

    def test_no_head_returns_400(self, client, monkeypatch):
        self._setup_meta(monkeypatch, None)
        resp = client.post("/checkout/" + "b" * 64)
        assert resp.status_code == 400

    def test_successful_checkout_returns_ok(self, client, monkeypatch):
        import engine.checkout as co_mod

        self._setup_meta(monkeypatch, "a" * 64)
        monkeypatch.setattr(co_mod, "apply_checkout", lambda *a, **kw: None)

        resp = client.post("/checkout/" + "b" * 64)
        assert resp.status_code == 200
        assert json.loads(resp.data).get("ok") is True

    def test_checkout_failure_returns_500_with_error(self, client, monkeypatch):
        import api.app as app_mod
        from engine.errors import CheckoutSchemaError

        self._setup_meta(monkeypatch, "a" * 64)

        def boom(*a, **kw):
            raise CheckoutSchemaError("DDL failed")

        # Must patch at api.app namespace since the route already imported apply_checkout
        monkeypatch.setattr(app_mod, "apply_checkout", boom)

        resp = client.post("/checkout/" + "b" * 64)
        assert resp.status_code == 500
        body = json.loads(resp.data)
        assert "error" in body
        assert "DDL failed" in body["error"]
