# PostgreSQL Driver Instruction

## 1. Purpose

`PostgresDriver` is a reusable class for external projects.  
It provides:
- connection management;
- CRUD operations;
- universal SQL execution via `execute_query()`;
- context manager support (`with` syntax).

## 2. Requirements

- Python 3.10+
- Installed packages from `requirements.txt`

Install dependencies:

```bash
pip install -r requirements.txt
```

## 3. Environment Variables

Copy `.env.example` to `.env` and fill in real values:

```bash
cp .env.example .env
```

Set these variables in `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=test
DB_USER=user
DB_PASSWORD=your_password
```

## 4. Quick Start

```python
from db_driver import PostgresDriver

with PostgresDriver() as db:
    rows = db.execute_query(
        "SELECT * FROM users LIMIT %s",
        params=(5,),
        fetch="all",
    )
    print(rows)
```

## 5. CRUD Methods

### create

```python
new_id = db.create("users", {"name": "Alice", "email": "alice@example.com"})
```

### read_one

```python
user = db.read_one("users", {"id": 1})
```

### read_many

```python
users = db.read_many("users", filters={"is_active": True}, limit=10)
```

### update

```python
updated = db.update("users", {"name": "Alice Smith"}, {"id": 1})
```

### delete

```python
deleted = db.delete("users", {"id": 1})
```

## 6. Universal Method `execute_query()`

Signature:

```python
execute_query(query, params=None, fetch="all", commit=None)
```

Parameters:
- `query`: SQL string or safe SQL object (`psycopg2.sql.Composable`);
- `params`: tuple/list of query parameters;
- `fetch`: `"all" | "one" | "none"`;
- `commit`: `True | False | None`.

Behavior:
- if `commit=None`, driver auto-commits non-`SELECT` queries;
- for `fetch="all"` returns `list[dict]`;
- for `fetch="one"` returns `dict | None`;
- for `fetch="none"` returns affected rows count (`rowcount`).

Examples:

```python
# SELECT one row
row = db.execute_query(
    "SELECT * FROM users WHERE id = %s",
    params=(1,),
    fetch="one",
)

# UPDATE with explicit commit
affected = db.execute_query(
    "UPDATE users SET is_active = %s WHERE id = %s",
    params=(False, 1),
    fetch="none",
    commit=True,
)
```

## 7. Context Manager

`PostgresDriver` supports `with` syntax:
- opens/ensures connection on enter;
- closes connection on exit.

```python
with PostgresDriver() as db:
    users = db.read_many("users", limit=3)
```

## 8. Safety Notes

- Always pass values through `params`, do not format values into SQL manually.
- For dynamic table/column names use `psycopg2.sql` (`sql.SQL`, `sql.Identifier`).
- The driver automatically handles commit/rollback in write operations.
