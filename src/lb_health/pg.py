"""Postgres connection factory with guardrails and query runners."""
from __future__ import annotations

import os
from contextlib import contextmanager

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
APPLICATION_NAME = "lakebase_health"


def load_sql(name: str) -> str:
    with open(os.path.join(SQL_DIR, name), encoding="utf-8") as f:
        return f.read()


def guardrail_statements(lock_timeout_ms: int, statement_timeout_ms: int) -> list[str]:
    return [
        "SET default_transaction_read_only = on",
        f"SET lock_timeout = '{int(lock_timeout_ms)}ms'",
        f"SET statement_timeout = '{int(statement_timeout_ms)}ms'",
        f"SET application_name = '{APPLICATION_NAME}'",
    ]


@contextmanager
def connect(host: str, dbname: str, user: str, password: str,
            lock_timeout_ms: int, statement_timeout_ms: int, connect_timeout_s: int = 15):
    """Open a read-only, time-boxed session.

    Session settings are applied in autocommit mode, then the connection is switched to
    read-only transactions: psycopg issues BEGIN READ ONLY before each query batch. Every
    query runs in its own short transaction (see run_query), so no lock is held longer
    than one statement.
    """
    import psycopg  # imported lazily so unit tests do not need it

    conn = psycopg.connect(host=host, dbname=dbname, user=user, password=password,
                           sslmode="require", connect_timeout=connect_timeout_s,
                           application_name=APPLICATION_NAME, autocommit=True)
    try:
        with conn.cursor() as cur:
            for stmt in guardrail_statements(lock_timeout_ms, statement_timeout_ms):
                cur.execute(stmt)
        conn.autocommit = False
        conn.read_only = True
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_query(conn, sql: str, statement_timeout_ms: int | None = None) -> list[dict]:
    """Run one query in its own READ ONLY transaction and return rows as dicts."""
    try:
        with conn.cursor() as cur:
            if statement_timeout_ms is not None:
                cur.execute(f"SET LOCAL statement_timeout = '{int(statement_timeout_ms)}ms'")
            cur.execute(sql)
            cols = [d.name for d in cur.description] if cur.description else []
            rows = [dict(zip(cols, r)) for r in cur.fetchall()] if cols else []
        conn.commit()
        return rows
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def scalar(conn, sql: str):
    rows = run_query(conn, sql)
    if not rows:
        return None
    return next(iter(rows[0].values()))
