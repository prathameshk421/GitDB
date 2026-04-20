from __future__ import annotations

from flask import Flask, jsonify, request, session
from flask_cors import CORS

from engine.db import load_repo_config, connect_meta_db, connect_target_db
import mysql.connector
from argon2 import PasswordHasher
from engine.diff import diff_snapshots, load_snapshot_from_row
from engine.checkout import apply_checkout
from engine.snapshot import capture_snapshot



def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = "gitdb-very-secret-key"  # TODO: use env var in prod
    CORS(app, supports_credentials=True)
    ph = PasswordHasher()

    def get_db():
        # Use config for now; in future, use per-user DB config
        return connect_meta_db(load_repo_config())

    @app.post("/login")
    def login():
        data = request.json
        username = data.get("username")
        password = data.get("password")
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT user_id, password_hash, is_active, full_name, email FROM user WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row or not row[2]:
            return jsonify({"error": "Invalid username or inactive user"}), 401
        try:
            ph.verify(row[1], password)
        except Exception:
            return jsonify({"error": "Invalid password"}), 401
        session["user_id"] = row[0]
        session["username"] = username
        return jsonify({"ok": True, "user": {"user_id": row[0], "username": username, "full_name": row[3], "email": row[4]}})

    @app.post("/logout")
    def logout():
        session.clear()
        return jsonify({"ok": True})

    @app.get("/me")
    def me():
        if "user_id" not in session:
            return jsonify({"user": None}), 401
        return jsonify({"user": {"user_id": session["user_id"], "username": session["username"]}})

    @app.get("/repositories")
    def repositories():
        if "user_id" not in session:
            return jsonify({"error": "Not authenticated"}), 401
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT repo_id, repo_name, target_db_name, db_host, db_port FROM repository WHERE user_id = %s", (session["user_id"],))
        repos = [
            {"repo_id": r[0], "repo_name": r[1], "db_name": r[2], "db_host": r[3], "db_port": r[4]} for r in cur.fetchall()
        ]
        return jsonify(repos)

    @app.get("/commits")
    def commits():
        cfg = load_repo_config()
        meta = connect_meta_db(cfg)
        cur = meta.cursor()
        cur.execute(
            """
            SELECT c.hash, c.parent_hash, c.message, c.created_at, u.username, u.full_name
            FROM commits c
            JOIN user u ON u.user_id = c.author_id
            WHERE c.repo_id = %s
            ORDER BY c.created_at DESC
            """,
            (cfg.repo_id,),
        )
        rows = [
            {
                "hash": r[0],
                "parent_hash": r[1],
                "message": r[2],
                "created_at": str(r[3]),
                "username": r[4],
                "full_name": r[5],
            }
            for r in cur.fetchall()
        ]
        return jsonify(rows)

    def _diff_impl(hash1: str, hash2: str):
        cfg = load_repo_config()
        meta = connect_meta_db(cfg)
        cur = meta.cursor()
        cur.execute(
            "SELECT table_name, ddl_json, rows_json FROM snapshots WHERE commit_hash = %s",
            (hash1,),
        )
        s1 = {r[0]: load_snapshot_from_row(r[1], r[2], r[0]) for r in cur.fetchall()}
        cur.execute(
            "SELECT table_name, ddl_json, rows_json FROM snapshots WHERE commit_hash = %s",
            (hash2,),
        )
        s2 = {r[0]: load_snapshot_from_row(r[1], r[2], r[0]) for r in cur.fetchall()}
        res = diff_snapshots(s1, s2)
        return jsonify(
            {
                "schema_sql": res.schema_sql,
                "data_sql": res.data_sql,
                "warnings": res.warnings,
            }
        )

    @app.get("/diff/<hash1>/<hash2>")
    def diff_get(hash1: str, hash2: str):
        return _diff_impl(hash1, hash2)

    @app.post("/diff/<hash1>/<hash2>")
    def diff_post(hash1: str, hash2: str):
        return _diff_impl(hash1, hash2)

    @app.get("/snapshot/<commit_hash>")
    def snapshot(commit_hash: str):
        cfg = load_repo_config()
        meta = connect_meta_db(cfg)
        cur = meta.cursor()
        cur.execute(
            "SELECT table_name, ddl_json, row_count FROM snapshots WHERE commit_hash = %s",
            (commit_hash,),
        )
        out = []
        import json as _json

        for table_name, ddl_json, row_count in cur.fetchall():
            out.append(
                {
                    "table_name": table_name,
                    "row_count": int(row_count),
                    "ddl": _json.loads(ddl_json),
                }
            )
        return jsonify(out)

    @app.post("/checkout/<commit_hash>")
    def checkout(commit_hash: str):
        cfg = load_repo_config()
        meta = connect_meta_db(cfg)
        target = connect_target_db(cfg)
        cur = meta.cursor()
        cur.execute("SELECT current_hash FROM repository WHERE repo_id = %s", (cfg.repo_id,))
        row = cur.fetchone()
        head = row[0] if row else None
        if not head:
            return jsonify({"error": "No HEAD commit found"}), 400

        def load(commit: str):
            c = meta.cursor()
            c.execute(
                "SELECT table_name, ddl_json, rows_json FROM snapshots WHERE commit_hash = %s",
                (commit,),
            )
            from engine.diff import load_snapshot_from_row

            out = {}
            for table_name, ddl_json, rows_json in c.fetchall():
                out[table_name] = load_snapshot_from_row(ddl_json, rows_json, table_name)
            return out

        old_snap = load(head)
        new_snap = load(commit_hash)
        res = diff_snapshots(old_snap, new_snap)
        recovery = None
        try:
            recovery = apply_checkout(target, diff=res, old_snapshot=old_snap)
        except Exception as e:
            payload = {"error": str(e)}
            if recovery:
                payload["recovery_file"] = recovery
            return jsonify(payload), 500

        cur2 = meta.cursor()
        cur2.execute(
            "UPDATE repository SET current_hash = %s WHERE repo_id = %s",
            (commit_hash, cfg.repo_id),
        )
        meta.commit()
        return jsonify({"ok": True})

    @app.get("/status")
    def status():
        cfg = load_repo_config()
        meta = connect_meta_db(cfg)
        target = connect_target_db(cfg)
        cur = meta.cursor()
        cur.execute("SELECT current_hash FROM repository WHERE repo_id = %s", (cfg.repo_id,))
        row = cur.fetchone()
        head = row[0] if row else None
        if not head:
            return jsonify({"error": "No commits yet"}), 400

        # Lightweight status: compare live snapshot vs HEAD snapshot
        live = capture_snapshot(target, cfg.db_name)

        cur.execute(
            "SELECT table_name, ddl_json, rows_json FROM snapshots WHERE commit_hash = %s",
            (head,),
        )
        from engine.diff import load_snapshot_from_row

        head_snap = {
            t: load_snapshot_from_row(ddl, rows, t) for (t, ddl, rows) in cur.fetchall()
        }

        added_tables = sorted(set(live.keys()) - set(head_snap.keys()))
        dropped_tables = sorted(set(head_snap.keys()) - set(live.keys()))
        modified_tables = sorted(
            t
            for t in set(live.keys()) & set(head_snap.keys())
            if live[t].ddl_json.get("raw_ddl") != head_snap[t].ddl_json.get("raw_ddl")
        )
        row_deltas = {
            t: {"head": head_snap[t].row_count, "live": live[t].row_count}
            for t in set(live.keys()) & set(head_snap.keys())
            if live[t].row_count != head_snap[t].row_count
        }

        return jsonify(
            {
                "added_tables": added_tables,
                "dropped_tables": dropped_tables,
                "modified_tables": modified_tables,
                "row_deltas": row_deltas,
            }
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)

