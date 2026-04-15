from __future__ import annotations

from getpass import getpass
from pathlib import Path
import json

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from argon2 import PasswordHasher
import keyring

from engine.db import (
    config_path,
    connect_mysql,
    gitdb_dir,
    keyring_service,
    keyring_username,
    load_repo_config,
    get_repo_password,
)
from engine.snapshot import capture_snapshot, snapshot_to_json
from engine.hash import compute_commit_hash
from engine.diff import DiffResult, diff_snapshots, load_snapshot_from_row
from engine.checkout import apply_checkout


console = Console()
ph = PasswordHasher()


def _ensure_schema(meta_conn) -> None:
    sql_path = Path("db/schema.sql")
    ddl = sql_path.read_text(encoding="utf-8")
    cur = meta_conn.cursor()
    # naive splitter; schema.sql contains only statement-level DDL
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        cur.execute(stmt)
    meta_conn.commit()


def _get_active_user_id(meta_conn, username: str) -> int | None:
    cur = meta_conn.cursor()
    cur.execute(
        "SELECT user_id FROM user WHERE username = %s AND is_active = TRUE",
        (username,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _get_repo_row(meta_conn, repo_id: int):
    cur = meta_conn.cursor()
    cur.execute(
        """
        SELECT repo_id, user_id, repo_name, target_db_name, db_host, db_port, db_user, current_hash
        FROM repository
        WHERE repo_id = %s
        """,
        (repo_id,),
    )
    return cur.fetchone()


def _get_head_hash(meta_conn, repo_id: int) -> str | None:
    cur = meta_conn.cursor()
    cur.execute("SELECT current_hash FROM repository WHERE repo_id = %s", (repo_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _set_head_hash(meta_conn, repo_id: int, new_hash: str | None) -> None:
    cur = meta_conn.cursor()
    cur.execute(
        "UPDATE repository SET current_hash = %s WHERE repo_id = %s",
        (new_hash, repo_id),
    )
    meta_conn.commit()


def _load_commit_snapshots(meta_conn, commit_hash: str) -> dict[str, "engine.snapshot.TableSnapshot"]:
    cur = meta_conn.cursor()
    cur.execute(
        "SELECT table_name, ddl_json, rows_json FROM snapshots WHERE commit_hash = %s",
        (commit_hash,),
    )
    out = {}
    for table_name, ddl_json, rows_json in cur.fetchall():
        out[table_name] = load_snapshot_from_row(ddl_json, rows_json, table_name)
    return out


@click.group()
def gitdb():
    """GitDB: Git-like version control for MySQL databases."""


@gitdb.command()
def register():
    """Create a GitDB user in gitdb_meta.user."""
    host = click.prompt("MySQL host", default="localhost")
    port = click.prompt("MySQL port", default=3306, type=int)
    user = click.prompt("MySQL user", default="root")
    password = getpass("MySQL password (will not echo): ")

    username = click.prompt("GitDB username")
    email = click.prompt("Email")
    full_name = click.prompt("Full name")
    gitdb_password = getpass("GitDB password (will not echo): ")

    # connect without db first, so we can create gitdb_meta if absent
    base = connect_mysql(host=host, port=port, user=user, password=password, database=None)
    cur0 = base.cursor()
    cur0.execute("CREATE DATABASE IF NOT EXISTS gitdb_meta")
    base.commit()
    base.close()

    conn = connect_mysql(host=host, port=port, user=user, password=password, database="gitdb_meta")
    _ensure_schema(conn)

    pw_hash = ph.hash(gitdb_password)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user (username, email, password_hash, full_name, is_active)
        VALUES (%s, %s, %s, %s, TRUE)
        """,
        (username, email, pw_hash, full_name),
    )
    conn.commit()
    console.print(f"[green]Registered user_id={cur.lastrowid}[/green]")


@gitdb.command()
@click.option("--host", required=True)
@click.option("--port", default=3306, type=int, show_default=True)
@click.option("--user", "db_user", required=True)
@click.option("--password", required=True)
@click.option("--database", "db_name", required=True)
@click.option("--repo-name", required=True)
@click.option("--author", required=True, help="GitDB username (must exist in gitdb_meta.user).")
def init(host: str, port: int, db_user: str, password: str, db_name: str, repo_name: str, author: str):
    """Initialize GitDB for a target MySQL database."""
    # Ensure gitdb_meta exists and schema is applied.
    base = connect_mysql(host=host, port=port, user=db_user, password=password, database=None)
    cur0 = base.cursor()
    cur0.execute("CREATE DATABASE IF NOT EXISTS gitdb_meta")
    base.commit()
    base.close()

    meta_conn = connect_mysql(host=host, port=port, user=db_user, password=password, database="gitdb_meta")
    _ensure_schema(meta_conn)

    author_id = _get_active_user_id(meta_conn, author)
    if not author_id:
        raise click.ClickException(f"Author '{author}' not found or inactive. Run `gitdb register`.")

    cur = meta_conn.cursor()
    cur.execute(
        """
        INSERT INTO repository (user_id, repo_name, target_db_name, db_host, db_port, db_user, db_password_key, current_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
        """,
        (author_id, repo_name, db_name, host, port, db_user, None),
    )
    meta_conn.commit()
    repo_id = int(cur.lastrowid)

    # Store password in OS keyring; write only non-secret config file.
    keyring.set_password(keyring_service(repo_id), keyring_username(repo_id), password)
    cur.execute(
        "UPDATE repository SET db_password_key = %s WHERE repo_id = %s",
        (keyring_username(repo_id), repo_id),
    )
    meta_conn.commit()

    gitdb_dir().mkdir(parents=True, exist_ok=True)
    config = {
        "repo_id": repo_id,
        "host": host,
        "port": port,
        "db_user": db_user,
        "db_name": db_name,
    }
    config_path().write_text(json.dumps(config, indent=2), encoding="utf-8")

    console.print(Panel.fit(f"[green]Initialized GitDB repo_id={repo_id}[/green]\nWrote `.gitdb/config.json`"))


@gitdb.command()
@click.option("-m", "--message", required=True)
@click.option("--author", required=True, help="GitDB username (must exist in gitdb_meta.user).")
def commit(message: str, author: str):
    """Create a commit: snapshot -> hash -> persist."""
    cfg = load_repo_config()
    pw = get_repo_password(cfg.repo_id)
    target_conn = connect_mysql(
        host=cfg.host,
        port=cfg.port,
        user=cfg.db_user,
        password=pw,
        database=cfg.db_name,
    )
    meta_conn = connect_mysql(
        host=cfg.host,
        port=cfg.port,
        user=cfg.db_user,
        password=pw,
        database="gitdb_meta",
    )

    author_id = _get_active_user_id(meta_conn, author)
    if not author_id:
        raise click.ClickException(f"Author '{author}' not found or inactive.")

    parent_hash = _get_head_hash(meta_conn, cfg.repo_id)
    snap = capture_snapshot(target_conn, cfg.db_name)
    commit_hash = compute_commit_hash(snap, parent_hash)

    cur = meta_conn.cursor()
    cur.execute(
        """
        INSERT INTO commits (hash, repo_id, parent_hash, author_id, message)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (commit_hash, cfg.repo_id, parent_hash, author_id, message),
    )

    # Persist snapshots per table
    for table, ts in snap.items():
        ddl_json = json.dumps(ts.ddl_json, sort_keys=True, default=str)
        rows_json = json.dumps(ts.rows_json, sort_keys=True, default=str)
        cur.execute(
            """
            INSERT INTO snapshots (commit_hash, table_name, ddl_json, rows_json, row_count)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (commit_hash, table, ddl_json, rows_json, ts.row_count),
        )
    meta_conn.commit()
    _set_head_hash(meta_conn, cfg.repo_id, commit_hash)
    console.print(f"[green]Committed {commit_hash}[/green]")


@gitdb.command()
@click.option("--oneline", is_flag=True, default=False)
@click.option("--graph", is_flag=True, default=False)
def log(oneline: bool, graph: bool):
    """Show commit history from HEAD walking parent_hash."""
    cfg = load_repo_config()
    pw = get_repo_password(cfg.repo_id)
    meta_conn = connect_mysql(host=cfg.host, port=cfg.port, user=cfg.db_user, password=pw, database="gitdb_meta")
    head = _get_head_hash(meta_conn, cfg.repo_id)
    if not head:
        console.print("[yellow]No commits yet.[/yellow]")
        return

    cur = meta_conn.cursor()
    rows = []
    h = head
    while h:
        cur.execute(
            """
            SELECT c.hash, c.parent_hash, c.message, c.created_at, u.full_name, u.username
            FROM commits c
            JOIN user u ON u.user_id = c.author_id
            WHERE c.hash = %s
            """,
            (h,),
        )
        r = cur.fetchone()
        if not r:
            break
        rows.append(r)
        h = r[1]

    if oneline:
        for (hash_, _parent, msg, _ts, _full, _usern) in rows:
            console.print(f"{hash_[:8]} {msg}")
        return

    table = Table(title="GitDB Log")
    table.add_column("Hash", style="cyan")
    table.add_column("Message")
    table.add_column("Author", style="green")
    table.add_column("Time", style="magenta")
    for (hash_, parent, msg, ts, full, usern) in rows:
        table.add_row(hash_[:12], msg, f"{full} ({usern})", str(ts))
    console.print(table)

    if graph:
        console.print("\n".join(["* " + r[0][:12] for r in rows]))


@gitdb.command()
@click.argument("hash1")
@click.argument("hash2")
@click.option("--schema-only", is_flag=True, default=False)
@click.option("--data-only", is_flag=True, default=False)
def diff(hash1: str, hash2: str, schema_only: bool, data_only: bool):
    """Compute diff between two commits."""
    if schema_only and data_only:
        raise click.ClickException("Use at most one of --schema-only or --data-only.")
    cfg = load_repo_config()
    pw = get_repo_password(cfg.repo_id)
    meta_conn = connect_mysql(host=cfg.host, port=cfg.port, user=cfg.db_user, password=pw, database="gitdb_meta")
    s1 = _load_commit_snapshots(meta_conn, hash1)
    s2 = _load_commit_snapshots(meta_conn, hash2)
    res = diff_snapshots(s1, s2)

    if res.warnings:
        console.print(Panel("\n".join(res.warnings), title="Warnings", style="yellow"))

    if not data_only:
        console.print(Panel("\n".join(res.schema_sql) or "(none)", title="Schema SQL", style="blue"))
    if not schema_only:
        console.print(Panel("\n".join(res.data_sql) or "(none)", title="Data SQL", style="blue"))


@gitdb.command()
@click.argument("commit_hash")
def checkout(commit_hash: str):
    """Checkout a commit hash into the live target database (two-phase)."""
    cfg = load_repo_config()
    pw = get_repo_password(cfg.repo_id)
    target_conn = connect_mysql(host=cfg.host, port=cfg.port, user=cfg.db_user, password=pw, database=cfg.db_name)
    meta_conn = connect_mysql(host=cfg.host, port=cfg.port, user=cfg.db_user, password=pw, database="gitdb_meta")

    head = _get_head_hash(meta_conn, cfg.repo_id)
    if not head:
        raise click.ClickException("No HEAD commit to diff from. Create a commit first.")

    old_snap = _load_commit_snapshots(meta_conn, head)
    new_snap = _load_commit_snapshots(meta_conn, commit_hash)
    res = diff_snapshots(old_snap, new_snap)

    try:
        recovery = apply_checkout(
            target_conn,
            diff=res,
            old_snapshot=old_snap,
        )
    except Exception as e:
        msg = str(e)
        console.print(Panel(msg, title="Checkout failed", style="red"))
        raise

    _set_head_hash(meta_conn, cfg.repo_id, commit_hash)
    if recovery:
        console.print(Panel(f"Recovery file: {recovery}", title="Recovery", style="yellow"))
    console.print(f"[green]Checked out {commit_hash}[/green]")


@gitdb.command()
def status():
    """Report uncommitted changes vs HEAD snapshot (lightweight: schema+row counts)."""
    cfg = load_repo_config()
    pw = get_repo_password(cfg.repo_id)
    target_conn = connect_mysql(host=cfg.host, port=cfg.port, user=cfg.db_user, password=pw, database=cfg.db_name)
    meta_conn = connect_mysql(host=cfg.host, port=cfg.port, user=cfg.db_user, password=pw, database="gitdb_meta")

    head = _get_head_hash(meta_conn, cfg.repo_id)
    if not head:
        console.print("[yellow]No commits yet.[/yellow]")
        return

    head_snap = _load_commit_snapshots(meta_conn, head)
    live_snap = capture_snapshot(target_conn, cfg.db_name)

    head_tables = set(head_snap.keys())
    live_tables = set(live_snap.keys())
    added = sorted(live_tables - head_tables)
    dropped = sorted(head_tables - live_tables)
    common = sorted(head_tables & live_tables)

    modified = []
    row_deltas = []
    for t in common:
        if head_snap[t].ddl_json.get("raw_ddl") != live_snap[t].ddl_json.get("raw_ddl"):
            modified.append(t)
        if head_snap[t].row_count != live_snap[t].row_count:
            row_deltas.append((t, head_snap[t].row_count, live_snap[t].row_count))

    lines = []
    if added:
        lines.append("Added tables: " + ", ".join(f"`{t}`" for t in added))
    if dropped:
        lines.append("Dropped tables: " + ", ".join(f"`{t}`" for t in dropped))
    if modified:
        lines.append("Modified tables: " + ", ".join(f"`{t}`" for t in modified))
    if row_deltas:
        lines.append("Row deltas:")
        for (t, a, b) in row_deltas:
            lines.append(f"  - {t}: {a} -> {b}")

    console.print(Panel("\n".join(lines) if lines else "Clean.", title="Status"))


if __name__ == "__main__":
    gitdb()

