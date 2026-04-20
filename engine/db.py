from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import os

import keyring
import mysql.connector

from .errors import ConfigError


def get_env_db_config():
    host = os.environ.get("GITDB_META_HOST")
    port = os.environ.get("GITDB_META_PORT", "3306")
    user = os.environ.get("GITDB_META_USER")
    password = os.environ.get("GITDB_META_PASSWORD")
    if host and user and password:
        return {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
        }
    return None


@dataclass(frozen=True)
class RepoConfig:
    repo_id: int | None
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
            repo_id=int(raw["repo_id"]) if raw.get("repo_id") else None,
            host=str(raw["host"]),
            port=int(raw.get("port", 3306)),
            db_user=str(raw["db_user"]),
            db_name=str(raw["db_name"]),
        )
    except Exception as e:
        raise ConfigError(f"Invalid .gitdb/config.json: {e}") from e


META_KEYRING_SERVICE = "gitdb_meta"
META_KEYRING_USER = "meta"


def keyring_service(repo_id: int) -> str:
    return "gitdb"


def keyring_username(repo_id: int) -> str:
    return f"repo_{repo_id}"


def get_meta_password_from_keyring() -> str | None:
    return keyring.get_password(META_KEYRING_SERVICE, META_KEYRING_USER)


def set_meta_password_in_keyring(password: str) -> None:
    keyring.set_password(META_KEYRING_SERVICE, META_KEYRING_USER, password)


def get_repo_password(repo_id: int) -> str:
    pw = keyring.get_password(keyring_service(repo_id), keyring_username(repo_id))
    if pw is None:
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


def get_meta_password(cfg: RepoConfig | None = None) -> str:
    if cfg and cfg.repo_id is not None:
        return get_repo_password(cfg.repo_id)
    env_cfg = get_env_db_config()
    if env_cfg:
        return env_cfg["password"]
    meta_pw = get_meta_password_from_keyring()
    if meta_pw is not None:
        return meta_pw
    raise ConfigError("Meta DB password not found. Run `gitdb register` first.")


def connect_meta_db(cfg: RepoConfig | None = None):
    if cfg:
        password = get_meta_password(cfg)
        return connect_mysql(
            host=cfg.host,
            port=cfg.port,
            user=cfg.db_user,
            password=password,
            database="gitdb_meta",
        )
    password = get_meta_password()
    env_cfg = get_env_db_config()
    if env_cfg:
        return connect_mysql(
            host=env_cfg["host"],
            port=env_cfg["port"],
            user=env_cfg["user"],
            password=password,
            database="gitdb_meta",
        )
    cfg = load_repo_config()
    return connect_mysql(
        host=cfg.host,
        port=cfg.port,
        user=cfg.db_user,
        password=password,
        database="gitdb_meta",
    )


def connect_meta_db_from_env():
    env_cfg = get_env_db_config()
    if env_cfg:
        return connect_mysql(
            host=env_cfg["host"],
            port=env_cfg["port"],
            user=env_cfg["user"],
            password=env_cfg["password"],
            database="gitdb_meta",
        )
    return None

