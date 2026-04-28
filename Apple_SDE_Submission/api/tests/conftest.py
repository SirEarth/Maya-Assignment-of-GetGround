"""
pytest fixtures for the API integration tests.

The integration suite drives the real FastAPI app via TestClient, which means
the lifespan hook initialises an asyncpg pool against the live PostgreSQL
database. Tests that exercise POST /pipeline and POST /load-data therefore
write rows to fact_price_offer, stg_price_offer, dq_bad_records, etc. against
the same `maya_assignment` database the dashboard reads from.

Without explicit cleanup the test residue (≈5 batches × 1 row each) leaks
into the user-facing dashboard right after `./start.sh`. The session-scoped
autouse fixture below wipes the transactional tables after the test session
finishes, leaving dim_* (Product Reference, partners, currencies) intact.
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def _wipe_test_residue_after_session():
    """Truncate transactional tables after the test session ends.

    Setup: nothing (yield immediately).
    Teardown: connect via psycopg2 (synchronous; the FastAPI asyncpg pool is
    already closed by lifespan teardown) and TRUNCATE the 8 transactional
    tables. dim_* seed data is preserved so the dashboard / next pytest run
    starts from a clean slate against an already-seeded reference catalogue.
    """
    yield

    # Session over — clean up. Use psycopg2 directly because asyncpg's pool
    # was closed when the last TestClient teardown ran lifespan shutdown.
    import psycopg2

    conn = psycopg2.connect(
        database = os.environ.get("PGDATABASE", "maya_assignment"),
        user     = os.environ.get("PGUSER", os.environ.get("USER", "postgres")),
        host     = os.environ.get("PGHOST", "localhost"),
        port     = int(os.environ.get("PGPORT", "5432")),
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE
                    dq_bad_records,
                    dq_output,
                    dws_partner_dq_per_batch,
                    fact_anomaly,
                    fact_payment_full_price,
                    fact_payment_instalment,
                    fact_partner_price_history,
                    fact_price_offer,
                    stg_price_offer
                RESTART IDENTITY CASCADE
                """
            )
    finally:
        conn.close()
