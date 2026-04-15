CREATE DATABASE IF NOT EXISTS gitdb_meta;
USE gitdb_meta;

-- Users (authentication + authorship)
CREATE TABLE IF NOT EXISTS user (
    user_id       INT          NOT NULL AUTO_INCREMENT,
    username      VARCHAR(50)  NOT NULL,
    email         VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name     VARCHAR(100) NOT NULL,
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    UNIQUE KEY uq_username (username),
    UNIQUE KEY uq_email    (email)
);

-- Repositories (connection config + HEAD pointer)
CREATE TABLE IF NOT EXISTS repository (
    repo_id         INT          NOT NULL AUTO_INCREMENT,
    user_id         INT          NOT NULL,
    repo_name       VARCHAR(100) NOT NULL,
    target_db_name  VARCHAR(100) NOT NULL,
    db_host         VARCHAR(100) NOT NULL,
    db_port         INT          NOT NULL DEFAULT 3306,
    db_user         VARCHAR(50)  NOT NULL,
    db_password_key VARCHAR(100) NULL,   -- keyring key, not the password
    current_hash    CHAR(64)     NULL,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id),
    CONSTRAINT fk_repo_user
        FOREIGN KEY (user_id) REFERENCES user (user_id)
);

-- Commits (linear Merkle chain)
CREATE TABLE IF NOT EXISTS commits (
    hash         CHAR(64) NOT NULL,
    repo_id      INT      NOT NULL,
    parent_hash  CHAR(64) NULL,
    author_id    INT      NOT NULL,
    message      TEXT     NOT NULL,
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hash),
    UNIQUE KEY uq_parent_hash (parent_hash),  -- enforces linear chain
    CONSTRAINT fk_commit_repo
        FOREIGN KEY (repo_id)     REFERENCES repository (repo_id),
    CONSTRAINT fk_commit_parent
        FOREIGN KEY (parent_hash) REFERENCES commits    (hash),
    CONSTRAINT fk_commit_author
        FOREIGN KEY (author_id)   REFERENCES user       (user_id)
);

-- Deferred FK: resolves circular dependency between repository and commits
ALTER TABLE repository
    ADD CONSTRAINT fk_repo_head
        FOREIGN KEY (current_hash) REFERENCES commits (hash);

-- Snapshots (full schema + data per table per commit)
CREATE TABLE IF NOT EXISTS snapshots (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    commit_hash  CHAR(64)     NOT NULL,
    table_name   VARCHAR(255) NOT NULL,
    ddl_json     LONGTEXT     NOT NULL,
    rows_json    LONGTEXT     NOT NULL,
    row_count    INT          NOT NULL,
    captured_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_commit_table (commit_hash, table_name),
    CONSTRAINT fk_snap_commit
        FOREIGN KEY (commit_hash) REFERENCES commits (hash)
);

