import os
from typing import Any, Literal

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.sql import Composable
from psycopg2.extras import RealDictCursor


class PostgresDriver:
    def __init__(
        self,
        host: str | None = None,
        port: str | int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        autoconnect: bool = True,
        load_env: bool = True,
    ) -> None:
        if load_env:
            load_dotenv()

        self._config: dict[str, Any] = {
            "host": host or os.getenv("DB_HOST"),
            "port": str(port or os.getenv("DB_PORT", "5432")),
            "dbname": dbname or os.getenv("DB_NAME"),
            "user": user or os.getenv("DB_USER"),
            "password": password or os.getenv("DB_PASSWORD"),
        }

        missing_vars = [
            env_name
            for env_name, value in {
                "DB_HOST": self._config["host"],
                "DB_NAME": self._config["dbname"],
                "DB_USER": self._config["user"],
                "DB_PASSWORD": self._config["password"],
            }.items()
            if not value
        ]
        if missing_vars:
            raise ValueError(
                f"Missing required connection settings: {', '.join(missing_vars)}"
            )

        self._conn = None
        if autoconnect:
            self.connect()

    def __enter__(self) -> "PostgresDriver":
        self._ensure_connection()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def connect(self) -> None:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self._config)

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    def _ensure_connection(self) -> None:
        if self._conn is None or self._conn.closed:
            self.connect()

    def create_tables(self) -> None:
        self._ensure_connection()
        create_users_sql = """
            CREATE TABLE IF NOT EXISTS users (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age  INT CHECK (age >= 0)
            );
        """
        create_orders_sql = """
            CREATE TABLE IF NOT EXISTS orders (
                id         SERIAL PRIMARY KEY,
                user_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                amount     NUMERIC(10,2) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """

        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(create_users_sql)
                cursor.execute(create_orders_sql)

    def add_user(self, name: str, age: int | None) -> int:
        self._ensure_connection()
        query = "INSERT INTO users (name, age) VALUES (%s, %s) RETURNING id;"

        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(query, (name, age))
                user_id = cursor.fetchone()
                if not user_id:
                    raise RuntimeError("Failed to insert user.")
                return int(user_id[0])

    def add_order(self, user_id: int, amount: float) -> int:
        self._ensure_connection()
        query = "INSERT INTO orders (user_id, amount) VALUES (%s, %s) RETURNING id;"

        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(query, (user_id, amount))
                order_id = cursor.fetchone()
                if not order_id:
                    raise RuntimeError("Failed to insert order.")
                return int(order_id[0])

    def get_user_by_name(self, name: str) -> dict[str, Any] | None:
        self._ensure_connection()
        query = """
            SELECT id, name, age
            FROM users
            WHERE name = %s
            LIMIT 1;
        """
        with self._conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (name,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def order_exists(self, user_id: int, amount: float) -> bool:
        self._ensure_connection()
        query = """
            SELECT 1
            FROM orders
            WHERE user_id = %s AND amount = %s
            LIMIT 1;
        """
        with self._conn.cursor() as cursor:
            cursor.execute(query, (user_id, amount))
            return cursor.fetchone() is not None

    def get_user_totals(self) -> list[dict[str, Any]]:
        self._ensure_connection()
        query = """
            SELECT
                u.id,
                u.name,
                COALESCE(SUM(o.amount), 0)::numeric(10,2) AS total_amount
            FROM users u
            LEFT JOIN orders o ON o.user_id = u.id
            GROUP BY u.id, u.name
            ORDER BY total_amount DESC, u.name ASC;
        """

        with self._conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def execute_query(
        self,
        query: str | Composable,
        params: tuple[Any, ...] | list[Any] | None = None,
        fetch: Literal["all", "one", "none"] = "all",
        commit: bool | None = None,
    ) -> Any:
        if fetch not in {"all", "one", "none"}:
            raise ValueError("fetch must be one of: 'all', 'one', 'none'.")

        self._ensure_connection()

        cursor_factory = RealDictCursor if fetch in {"all", "one"} else None
        query_text = query.as_string(self._conn) if isinstance(query, Composable) else query
        normalized_query = query_text.strip().lower()
        is_read_query = normalized_query.startswith("select")
        should_commit = (not is_read_query) if commit is None else commit

        with self._conn.cursor(cursor_factory=cursor_factory) as cursor:
            try:
                cursor.execute(query, params)

                if fetch == "one":
                    row = cursor.fetchone()
                    result = dict(row) if row else None
                elif fetch == "all":
                    rows = cursor.fetchall()
                    result = [dict(row) for row in rows]
                else:
                    result = cursor.rowcount

                if should_commit:
                    self._conn.commit()
                return result
            except Exception:
                if should_commit:
                    self._conn.rollback()
                raise

    def create(self, table: str, data: dict[str, Any]) -> int | None:
        if not data:
            raise ValueError("Data for create() must not be empty.")

        self._ensure_connection()

        columns = list(data.keys())
        values = list(data.values())
        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING id"
        ).format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            values=sql.SQL(", ").join(sql.Placeholder() for _ in values),
        )

        with self._conn.cursor() as cursor:
            try:
                cursor.execute(query, values)
                new_id = cursor.fetchone()
                self._conn.commit()
                return new_id[0] if new_id else None
            except Exception:
                self._conn.rollback()
                raise

    def read_one(self, table: str, filters: dict[str, Any]) -> dict[str, Any] | None:
        if not filters:
            raise ValueError("Filters for read_one() must not be empty.")

        self._ensure_connection()

        where_clause = sql.SQL(" AND ").join(
            sql.SQL("{} = {}").format(sql.Identifier(key), sql.Placeholder())
            for key in filters.keys()
        )
        query = sql.SQL("SELECT * FROM {table} WHERE {where} LIMIT 1").format(
            table=sql.Identifier(table),
            where=where_clause,
        )

        with self._conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, list(filters.values()))
            row = cursor.fetchone()
            return dict(row) if row else None

    def read_many(
        self,
        table: str,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        self._ensure_connection()

        base_query = sql.SQL("SELECT * FROM {table}").format(
            table=sql.Identifier(table)
        )
        params: list[Any] = []

        if filters:
            where_clause = sql.SQL(" AND ").join(
                sql.SQL("{} = {}").format(sql.Identifier(key), sql.Placeholder())
                for key in filters.keys()
            )
            base_query = sql.SQL("{query} WHERE {where}").format(
                query=base_query,
                where=where_clause,
            )
            params.extend(filters.values())

        if limit is not None:
            if limit <= 0:
                raise ValueError("Limit must be greater than 0.")
            base_query = sql.SQL("{query} LIMIT {limit}").format(
                query=base_query,
                limit=sql.Literal(limit),
            )

        with self._conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(base_query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def update(self, table: str, data: dict[str, Any], filters: dict[str, Any]) -> int:
        if not data:
            raise ValueError("Data for update() must not be empty.")
        if not filters:
            raise ValueError("Filters for update() must not be empty.")

        self._ensure_connection()

        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.Identifier(key), sql.Placeholder())
            for key in data.keys()
        )
        where_clause = sql.SQL(" AND ").join(
            sql.SQL("{} = {}").format(sql.Identifier(key), sql.Placeholder())
            for key in filters.keys()
        )
        query = sql.SQL("UPDATE {table} SET {set_clause} WHERE {where_clause}").format(
            table=sql.Identifier(table),
            set_clause=set_clause,
            where_clause=where_clause,
        )
        params = list(data.values()) + list(filters.values())

        with self._conn.cursor() as cursor:
            try:
                cursor.execute(query, params)
                affected_rows = cursor.rowcount
                self._conn.commit()
                return affected_rows
            except Exception:
                self._conn.rollback()
                raise

    def delete(self, table: str, filters: dict[str, Any]) -> int:
        if not filters:
            raise ValueError("Filters for delete() must not be empty.")

        self._ensure_connection()

        where_clause = sql.SQL(" AND ").join(
            sql.SQL("{} = {}").format(sql.Identifier(key), sql.Placeholder())
            for key in filters.keys()
        )
        query = sql.SQL("DELETE FROM {table} WHERE {where_clause}").format(
            table=sql.Identifier(table),
            where_clause=where_clause,
        )

        with self._conn.cursor() as cursor:
            try:
                cursor.execute(query, list(filters.values()))
                affected_rows = cursor.rowcount
                self._conn.commit()
                return affected_rows
            except Exception:
                self._conn.rollback()
                raise
