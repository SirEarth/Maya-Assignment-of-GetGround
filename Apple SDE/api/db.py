"""
PostgreSQL connection pool for the FastAPI application.

Initialised at app startup via the lifespan hook in main.py and shared
across all request handlers.

Configuration via environment variables (with sensible defaults for the
local Postgres.app demo):
    PGDATABASE   default: maya_assignment
    PGUSER       default: $USER
    PGHOST       default: localhost
    PGPORT       default: 5432
    PG_POOL_MIN  default: 2
    PG_POOL_MAX  default: 10
"""

from __future__ import annotations

import os
from typing import Optional

import asyncpg


_pool: Optional[asyncpg.Pool] = None


async def init_pool() -> None:
    """Build the connection pool. Call once at app startup."""
    global _pool
    if _pool is not None:
        return
    _pool = await asyncpg.create_pool(
        database = os.getenv("PGDATABASE", "maya_assignment"),
        user     = os.getenv("PGUSER", os.environ.get("USER", "postgres")),
        host     = os.getenv("PGHOST", "localhost"),
        port     = int(os.getenv("PGPORT", "5432")),
        min_size = int(os.getenv("PG_POOL_MIN", "2")),
        max_size = int(os.getenv("PG_POOL_MAX", "10")),
    )


async def close_pool() -> None:
    """Close the connection pool. Call once at app shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    """Return the shared pool. Raises RuntimeError if init_pool() wasn't called."""
    if _pool is None:
        raise RuntimeError("Connection pool not initialised — did lifespan run?")
    return _pool
