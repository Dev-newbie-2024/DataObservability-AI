"""
backend/core/database.py
========================
Snowflake connection pool manager.

Provides a thread-safe connection pool using snowflake-connector-python.
All other modules obtain connections via `get_connection()` context manager.

Design decisions:
  - SnowflakePool is a singleton (one pool per process).
  - Connections are validated before being returned (ping via SELECT 1).
  - Credentials are sourced exclusively from SnowflakeSettings (env-driven).
  - On failure the pool raises SnowflakeConnectionError (never crashes the app).
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection

from backend.core.exceptions import SnowflakeConnectionError, SnowflakeQueryError
from config.logging_config import get_logger
from config.settings import snowflake_settings

logger = get_logger(__name__)

# ─── Connection Pool ──────────────────────────────────────────────────────────

class SnowflakePool:
    """
    Thread-safe Snowflake connection pool.

    Attributes:
        _pool:      Queue holding idle connections.
        _size:      Fixed pool size (default 5).
        _lock:      Protects pool initialisation.
        _initialised: Guards against double-init.
    """

    def __init__(self, pool_size: int = 5) -> None:
        self._pool: queue.Queue[SnowflakeConnection] = queue.Queue(maxsize=pool_size)
        self._size = pool_size
        self._lock = threading.Lock()
        self._initialised = False

    def initialise(self) -> None:
        """Create all connections up front. Call once at app startup."""
        with self._lock:
            if self._initialised:
                return
            logger.info(
                "Initialising Snowflake connection pool",
                pool_size=self._size,
                account=snowflake_settings.account,
                database=snowflake_settings.database,
            )
            try:
                for _ in range(self._size):
                    conn = self._new_connection()
                    self._pool.put(conn)
                self._initialised = True
                logger.info("Snowflake pool ready", pool_size=self._size)
            except Exception as exc:
                raise SnowflakeConnectionError(
                    message="Failed to initialise Snowflake connection pool",
                    detail=str(exc),
                ) from exc

    def _new_connection(self) -> SnowflakeConnection:
        """Open a fresh connection using current settings."""
        return snowflake.connector.connect(
            account=snowflake_settings.account,
            user=snowflake_settings.user,
            password=snowflake_settings.password,
            database=snowflake_settings.database,
            warehouse=snowflake_settings.warehouse,
            role=snowflake_settings.role,
            schema=snowflake_settings.schema_,
            # Connection settings
            login_timeout=30,
            network_timeout=60,
            client_session_keep_alive=True,
        )

    def _is_alive(self, conn: SnowflakeConnection) -> bool:
        """Return True if the connection is still usable."""
        try:
            conn.cursor().execute("SELECT 1")
            return True
        except Exception:
            return False

    @contextmanager
    def acquire(self) -> Generator[SnowflakeConnection, None, None]:
        """
        Context manager that yields a live connection from the pool.
        The connection is returned to the pool on exit.
        If the connection is dead, a new one is created.

        Usage::

            with pool.acquire() as conn:
                cursor = conn.cursor(DictCursor)
                cursor.execute("SELECT ...")
        """
        try:
            conn = self._pool.get(timeout=15)
        except queue.Empty as exc:
            raise SnowflakeConnectionError(
                message="Connection pool exhausted — no idle connections available",
                detail="All connections are in use. Try again shortly.",
            ) from exc

        # Replace dead connections silently
        if not self._is_alive(conn):
            logger.warning("Replacing dead Snowflake connection")
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = self._new_connection()
            except Exception as exc:
                raise SnowflakeConnectionError(
                    message="Could not reconnect to Snowflake",
                    detail=str(exc),
                ) from exc

        try:
            yield conn
        finally:
            self._pool.put(conn)

    def close_all(self) -> None:
        """Close all pooled connections. Call at app shutdown."""
        logger.info("Closing Snowflake connection pool")
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Exception:
                pass
        self._initialised = False
        logger.info("Snowflake pool closed")


# ─── Module-level singleton ───────────────────────────────────────────────────

_pool: SnowflakePool | None = None
_pool_lock = threading.Lock()


def get_pool() -> SnowflakePool:
    """Return the module-level pool singleton (creates it if needed)."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = SnowflakePool(pool_size=5)
    return _pool


def initialise_pool() -> None:
    """Initialise the pool at application startup."""
    get_pool().initialise()


def close_pool() -> None:
    """Close the pool at application shutdown."""
    if _pool is not None:
        _pool.close_all()


@contextmanager
def get_connection() -> Generator[SnowflakeConnection, None, None]:
    """
    Convenience context manager — use this everywhere instead of the pool directly.

    Usage::

        from backend.core.database import get_connection
        with get_connection() as conn:
            cursor = conn.cursor(DictCursor)
            cursor.execute("SELECT ...")
    """
    with get_pool().acquire() as conn:
        yield conn


def execute_query(
    sql: str,
    params: tuple[Any, ...] | None = None,
    *,
    as_dict: bool = True,
) -> list[dict[str, Any]]:
    """
    Execute a SELECT query and return all rows.

    Args:
        sql:     SQL string (use %s for bind parameters).
        params:  Optional tuple of bind parameter values.
        as_dict: If True, return list[dict]; otherwise list[tuple].

    Returns:
        List of rows (dict or tuple depending on as_dict).
    """
    with get_connection() as conn:
        cursor_class = DictCursor if as_dict else conn.cursor().__class__
        cur = conn.cursor(DictCursor) if as_dict else conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()  # type: ignore[return-value]
        except Exception as exc:
            raise SnowflakeQueryError(
                message="Query execution failed",
                detail=str(exc),
            ) from exc
        finally:
            cur.close()


def execute_statement(
    sql: str,
    params: tuple[Any, ...] | None = None,
) -> int:
    """
    Execute a non-SELECT statement (INSERT / UPDATE / DELETE / DDL).

    Returns:
        Number of rows affected (0 for DDL).
    """
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.rowcount or 0
        except Exception as exc:
            raise SnowflakeQueryError(
                message="Statement execution failed",
                detail=str(exc),
            ) from exc
        finally:
            cur.close()
