"""
Shared fixtures for integration tests.

Integration tests require a live MySQL 8 instance.
They are skipped automatically when the environment variable
GITDB_TEST_MYSQL_URL is NOT set.

Export format:
    GITDB_TEST_MYSQL_URL=mysql://root:secret@localhost:3306

The fixtures create/drop a fresh test database per test session.
"""
from __future__ import annotations

import os
import re
import uuid

import pytest

# ── Skip guard ────────────────────────────────────────────────────────────────

MYSQL_URL_ENV = "GITDB_TEST_MYSQL_URL"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require a live MySQL instance (deselect with -m 'not integration')",
    )


def _parse_url(url: str):
    """Very simple mysql:// URL parser → (user, password, host, port)."""
    m = re.match(r"mysql://([^:]+):([^@]*)@([^:/]+):(\d+)", url)
    if not m:
        raise ValueError(f"Cannot parse GITDB_TEST_MYSQL_URL: {url!r}. Expected mysql://user:pass@host:port")
    user, password, host, port = m.group(1), m.group(2), m.group(3), int(m.group(4))
    return user, password, host, port


@pytest.fixture(scope="session")
def mysql_params():
    url = os.environ.get(MYSQL_URL_ENV)
    if not url:
        pytest.skip(f"Set {MYSQL_URL_ENV}=mysql://user:pass@host:port to run integration tests")
    user, password, host, port = _parse_url(url)
    return {"user": user, "password": password, "host": host, "port": port}


@pytest.fixture()
def test_db(mysql_params):
    """
    Create a fresh MySQL database for a single test, drop it afterwards.
    Yields (conn_to_target_db, db_name).
    """
    import mysql.connector

    db_name = f"gitdb_inttest_{uuid.uuid4().hex[:12]}"
    root_conn = mysql.connector.connect(**mysql_params, database=None, autocommit=True)
    root_cur = root_conn.cursor()
    root_cur.execute(f"CREATE DATABASE `{db_name}`")
    root_cur.close()
    root_conn.close()

    conn = mysql.connector.connect(**mysql_params, database=db_name, autocommit=False)
    yield conn, db_name

    conn.close()

    cleanup_conn = mysql.connector.connect(**mysql_params, database=None, autocommit=True)
    cleanup_cur = cleanup_conn.cursor()
    cleanup_cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    cleanup_cur.close()
    cleanup_conn.close()


@pytest.fixture()
def meta_db(mysql_params):
    """
    Create a fresh gitdb_meta database for a single test, drop it afterwards.
    Also runs db/schema.sql to set up the tables.
    Yields conn_to_gitdb_meta.
    """
    import mysql.connector
    from pathlib import Path

    db_name = f"gitdb_meta_test_{uuid.uuid4().hex[:12]}"
    root_conn = mysql.connector.connect(**mysql_params, database=None, autocommit=True)
    root_cur = root_conn.cursor()
    root_cur.execute(f"CREATE DATABASE `{db_name}`")
    root_cur.close()
    root_conn.close()

    conn = mysql.connector.connect(**mysql_params, database=db_name, autocommit=False)

    # Apply schema (strip the CREATE DATABASE / USE statements since we already selected the DB)
    schema_path = Path(__file__).parents[2] / "db" / "schema.sql"
    ddl = schema_path.read_text(encoding="utf-8")
    cur = conn.cursor()
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        upper = stmt.upper()
        if upper.startswith("CREATE DATABASE") or upper.startswith("USE "):
            continue
        try:
            cur.execute(stmt)
        except Exception:
            pass  # e.g. ALTER TABLE for FK that already exists idempotently
    conn.commit()

    yield conn, db_name

    conn.close()

    cleanup_conn = mysql.connector.connect(**mysql_params, database=None, autocommit=True)
    cleanup_cur = cleanup_conn.cursor()
    cleanup_cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    cleanup_cur.close()
    cleanup_conn.close()
