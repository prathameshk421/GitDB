# GitDB Meta Database — ER Diagram

## Database: `gitdb_meta`

---

## Entity: **User**

| Attribute | Type | Constraints |
|-----------|------|-------------|
| user_id | INT | PK, AUTO_INCREMENT |
| username | VARCHAR(50) | NOT NULL, UNIQUE |
| email | VARCHAR(100) | NOT NULL, UNIQUE |
| password_hash | VARCHAR(255) | NOT NULL |
| full_name | VARCHAR(100) | NOT NULL |
| is_active | BOOLEAN | NOT NULL, DEFAULT TRUE |
| created_at | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP |

---

## Entity: **Repository**

| Attribute | Type | Constraints |
|-----------|------|-------------|
| repo_id | INT | PK, AUTO_INCREMENT |
| user_id | INT | FK -> User(user_id), NOT NULL |
| repo_name | VARCHAR(100) | NOT NULL |
| target_db_name | VARCHAR(100) | NOT NULL |
| db_host | VARCHAR(100) | NOT NULL |
| db_port | INT | NOT NULL, DEFAULT 3306 |
| db_user | VARCHAR(50) | NOT NULL |
| db_password_key | VARCHAR(100) | NULL (keyring key) |
| current_hash | CHAR(64) | FK -> Commits(hash), NULL (deferred) |
| created_at | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP |

---

## Entity: **Commits**

| Attribute | Type | Constraints |
|-----------|------|-------------|
| hash | CHAR(64) | PK |
| repo_id | INT | FK -> Repository(repo_id), NOT NULL |
| parent_hash | CHAR(64) | FK -> Commits(hash), UNIQUE (enforces linear chain) |
| author_id | INT | FK -> User(user_id), NOT NULL |
| message | TEXT | NOT NULL |
| created_at | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP |

---

## Entity: **Snapshots**

| Attribute | Type | Constraints |
|-----------|------|-------------|
| id | BIGINT | PK, AUTO_INCREMENT |
| commit_hash | CHAR(64) | FK -> Commits(hash), UNIQUE(commit_hash, table_name) |
| table_name | VARCHAR(255) | NOT NULL |
| ddl_json | LONGTEXT | NOT NULL (JSON: schema, columns, raw DDL) |
| rows_json | LONGTEXT | NOT NULL (JSON: row data) |
| row_count | INT | NOT NULL |
| captured_at | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP |

---

## Relationships

| Relationship | From -> To | Cardinality | Description |
|--------------|------------|-------------|-------------|
| `authors` | User -> Commits | 1:N | A user authors many commits |
| `contains` | Repository -> Commits | 1:N | A repo has many commits (HEAD chain) |
| `parent_of` | Commits -> Commits | 1:1 (self) | Parent commit (linear chain, nullable) |
| `captured_in` | Commits -> Snapshots | 1:N | Each commit has snapshots per table |
| `head_pointed_by` | Repository -> Commits | 1:1 (deferred FK) | Repository's current HEAD pointer |

---

## ER Diagram (ASCII)

```
+-------------------+         +-------------------+
|       User         |         |    Repository      |
+-------------------+         +-------------------+
| PK  user_id (INT) |<--1:N---| FK  user_id (INT)  |
| UK  username       |         | PK  repo_id (INT) |
| UK  email         |         | repo_name          |
|     password_hash |         | target_db_name     |
|     full_name     |         | db_host            |
|     is_active     |         | db_port           |
|     created_at    |         | db_user           |
+-------------------+         | db_password_key   |
        | 1:N                | FK  current_hash ---+--+
        |                    | created_at          |  |
        |                    +-------------------+  |
        |                                         | 1:1
        v 1:N                                     v
+-------------------+         +-------------------+
|      Commits      |         |     Snapshots      |
+-------------------+         +-------------------+
| PK  hash (CHAR 64)|<---1:N--| FK  commit_hash    |
| FK  repo_id        |         | PK  id (BIGINT)    |
| FK  parent_hash ---+-------->| UK  (commit_hash,  |
| FK  author_id      |  self   |      table_name)  |
|     message       |  (1:1)  | table_name        |
|     created_at    |         | ddl_json          |
+-------------------+         | rows_json         |
                                | row_count         |
                                | captured_at       |
                                +-------------------+
```

---

## Key Notes

- **Circular FK**: `repository.current_hash -> commits(hash)` is added via deferred ALTER after `commits` table is created
- **Linear chain**: `commits.parent_hash` has UNIQUE constraint, enforcing one parent per commit (no merge branching)
- **Snapshots**: One row per table per commit; stores full schema (DDL JSON) + all data (rows JSON) separately
- **Keyring**: `db_password_key` stores a keyring key, NOT the actual password (security by OS keyring)
- **Password hashing**: Uses Argon2 (`argon2` library) for secure password storage in `user.password_hash`