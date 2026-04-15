## GitDB (DISL Mini Project)

GitDB is a **state-based, Git-like version control system for MySQL databases**.
It snapshots schema + data into a dedicated metadata database (`gitdb_meta`), then
computes diffs and performs safe two-phase checkouts (DDL first, then DML in a
transaction).

This repo matches the project documentation in `project_documentation/`, especially
`implementation_plan.tex`.

## Requirements

- **Python**: 3.10+
- **Node**: 18+
- **MySQL**: 8.0+

GitDB stores the MySQL password in the **OS keyring** (via `keyring`) and writes
only non-secret config to `.gitdb/config.json`.

## Setup (Python)

From repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Setup (MySQL)

Ensure you can connect to your MySQL server with a user that can create databases
and tables (at minimum: create `gitdb_meta` and read the target DB).

## CLI usage

### 1) Register a GitDB user

```bash
gitdb register
```

This prompts for MySQL connection details (to create `gitdb_meta` if needed) and
creates a row in `gitdb_meta.user` using an argon2 password hash.

### 2) Initialize GitDB for a target database

```bash
gitdb init --host localhost --port 3306 \
  --user root --password "<mysql_password>" \
  --database "<target_db_name>" \
  --repo-name "<repo_display_name>" \
  --author "<gitdb_username>"
```

This creates the metadata schema (`db/schema.sql`), registers a repository row,
stores the MySQL password in the OS keyring, and writes `.gitdb/config.json`.

### 3) Commit

```bash
gitdb commit -m "initial snapshot" --author "<gitdb_username>"
```

### 4) Log

```bash
gitdb log
gitdb log --oneline --graph
```

### 5) Diff

```bash
gitdb diff <hash1> <hash2>
gitdb diff <hash1> <hash2> --schema-only
gitdb diff <hash1> <hash2> --data-only
```

### 6) Checkout

```bash
gitdb checkout <commit_hash>
```

Checkout is **two-phase**:
- **Phase 1 (schema)**: applies DDL; on failure emits a recovery SQL file in `.gitdb/`.
- **Phase 2 (data)**: applies DML inside a transaction with `FOREIGN_KEY_CHECKS=0/1`.

### 7) Status

```bash
gitdb status
```

Reports added/dropped tables, modified tables (DDL change), and row-count deltas
vs the current HEAD snapshot.

## Flask API

Run:

```bash
python -m api.app
```

Endpoints:
- `GET /commits`
- `GET /diff/<h1>/<h2>` (also supports `POST`)
- `POST /checkout/<hash>`
- `GET /status`
- `GET /snapshot/<hash>` (DDL JSON per table)

## React UI

From `ui/`:

```bash
npm install
npm run dev
```

The UI expects the Flask API at `http://127.0.0.1:5000` (override with `VITE_API_BASE`).

## Tests

```bash
pytest
```

