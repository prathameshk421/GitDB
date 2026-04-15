from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import keyring
import mysql.connector

from .errors import ConfigError


@dataclass(frozen=True)
class RepoConfig:
    repo_id: int
    host: str
    port: int
    db_user: str
    db_name: str


def gitdb_dir() -> Path:
    return Path(".gitdb")


def config_path() -> Path:
    return gitdb_dir() / "config.json"


def load_repo_config() -> RepoConfig:
    p = config_path()
    if not p.exists():
        raise ConfigError("Missing .gitdb/config.json. Run `gitdb init` first.")
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return RepoConfig(
            repo_id=int(raw["repo_id"]),
            host=str(raw["host"]),
            port=int(raw.get("port", 3306)),
            db_user=str(raw["db_user"]),
            db_name=str(raw["db_name"]),
        )
    except Exception as e:
        raise ConfigError(f"Invalid .gitdb/config.json: {e}") from e


def keyring_service(repo_id: int) -> str:
    return "gitdb"


def keyring_username(repo_id: int) -> str:
    return f"repo_{repo_id}"


def get_repo_password(repo_id: int) -> str:
    pw = keyring.get_password(keyring_service(repo_id), keyring_username(repo_id))
    if not pw:
        raise ConfigError(
            "Database password not found in OS keyring. Re-run `gitdb init`."
        )
    return pw


def connect_mysql(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str | None,
):
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
    )


def connect_target_db(cfg: RepoConfig):
    return connect_mysql(
        host=cfg.host,
        port=cfg.port,
        user=cfg.db_user,
        password=get_repo_password(cfg.repo_id),
        database=cfg.db_name,
    )


def connect_meta_db(cfg: RepoConfig):
    # gitdb_meta lives on same server; uses same credentials.
    return connect_mysql(
        host=cfg.host,
        port=cfg.port,
        user=cfg.db_user,
        password=get_repo_password(cfg.repo_id),
        database="gitdb_meta",
    )

