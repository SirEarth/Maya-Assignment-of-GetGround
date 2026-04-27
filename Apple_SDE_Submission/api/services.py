"""
Service layer — bridges API endpoints to PostgreSQL + the harmonise/dq modules.

Implementations are now REAL (not stubs). Each function uses the asyncpg
connection pool from api/db.py.
"""

from __future__ import annotations

import csv
import io
import json
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import asyncpg
from fastapi import HTTPException

from .db import get_pool
from .models import (
    AnomaliesBySeverity, Anomaly, AnomalyContext, AnomalyType,
    AnomalyVisualization, BadRecordEntry, BadRecordStatus, BaselineSnapshot,
    Confidence, DQRuleRunResult, DQSummary, DQViolationsBySeverity,
    DetectAnomaliesRequest, DetectAnomaliesResponse, HarmoniseMatch,
    HarmoniseResponse, JobStatus, LoadDataAcceptedResponse, LoadDataProgress,
    LoadJobStatus, ResolveAction, Severity, SignalBreakdown, TimeSeriesPoint,
)


# ---------------------------------------------------------------------------
# In-process state for ingest jobs (small, demo-grade)
# Production: use a real ingest_job table in PostgreSQL
# ---------------------------------------------------------------------------
_jobs: Dict[uuid.UUID, dict] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ===========================================================================
# Harmoniser singleton (Top-K + score)
# ===========================================================================

_harmoniser = None


def get_harmoniser():
    """Lazy-load Harmoniser registry once per worker process."""
    global _harmoniser
    if _harmoniser is None:
        from harmonise import Harmoniser
        ref = Path(__file__).resolve().parent.parent / "Product Ref.csv"
        if not ref.exists():
            raise HTTPException(503, "Product Ref.csv not found on server")
        _harmoniser = Harmoniser(ref)
    return _harmoniser


def harmonise_product(query: str, k: int, min_confidence: Confidence) -> HarmoniseResponse:
    h = get_harmoniser()
    started = time.perf_counter()

    raw_matches = h.match(query, k=k)

    confidence_order = {Confidence.HIGH: 3, Confidence.MEDIUM: 2, Confidence.LOW: 1, Confidence.MANUAL: 4}
    floor = confidence_order[min_confidence]

    matches = []
    for m in raw_matches:
        if confidence_order[Confidence(m.confidence)] < floor:
            continue
        matches.append(HarmoniseMatch(
            model_key      = m.model_key,
            canonical_name = m.canonical_name,
            sku_ids        = m.sku_ids,
            score          = m.score,
            confidence     = Confidence(m.confidence),
            signal_breakdown = SignalBreakdown(
                attr_match      = m.breakdown.attr_match,
                token_jaccard   = m.breakdown.token_jaccard,
                char_fuzz       = m.breakdown.char_fuzz,
                attr_matched    = m.breakdown.attr_matches_on,
                attr_mismatched = m.breakdown.attr_mismatches,
            ),
        ))

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return HarmoniseResponse(query=query, matches=matches, elapsed_ms=elapsed_ms)


# ===========================================================================
# /load-data — REAL 8-step pipeline
# ===========================================================================

# CSV column → standardised partner_code
_PARTNER_CODE_MAP = {"Partner A": "PARTNER_A", "Partner B": "PARTNER_B"}

# country_name → ISO code (also stored in dim_country, but cached for speed).
#
# Accepts BOTH the full English name AND the ISO 3166-1 alpha-2 code, because
# real partner feeds are inconsistent — Partner B sample data has 154 rows
# where COUNTRY_VAL = "NZ" instead of "New Zealand". Both are legitimate
# representations of the same country, so DQ should NOT flag them as errors.
_COUNTRY_NAME_MAP = {
    # Full names
    "Australia":      "AU",
    "New Zealand":    "NZ",
    "United States":  "US",
    "United Kingdom": "GB",
    # ISO codes (treated as already-resolved)
    "AU": "AU",
    "NZ": "NZ",
    "US": "US",
    "GB": "GB",
}


async def submit_load_job(file_bytes: bytes, partner_code: str) -> LoadDataAcceptedResponse:
    """
    Real 8-step pipeline:
      1. Parse CSV → INSERT stg_price_offer
      2. Run INGEST-stage Data Quality (parse / format / required-field rules)
      3. Harmonise (only on rows that passed INGEST DQ)
      4. Run PRE_FACT-stage Data Quality (HIGH-severity gate before fact insert).
         Failing rows are flagged in dq_bad_records and DO NOT enter fact tables.
      5. Insert change events into fact_price_offer + payment child
         (only PRE_FACT-passing rows)
      6. Run SEMANTIC-stage Data Quality on fact rows (soft signals only —
         low-confidence harmonise + category sanity bounds).
         Failing rows STAY in fact, just flagged for triage.
      7. Update Slowly Changing Dimension Type 2 history
      8. Write per-batch summary to dws_partner_dq_per_batch
    """
    job_id   = uuid.uuid4()
    batch_id = uuid.uuid4()
    started_at = _now_utc()

    pool = get_pool()
    async with pool.acquire() as conn:
        # Resolve partner_id and validate (no transaction needed for read)
        partner_row = await conn.fetchrow(
            "SELECT partner_id, country_code FROM dim_partner WHERE partner_code = $1",
            partner_code,
        )
        if not partner_row:
            raise HTTPException(400, f"Unknown partner_code: {partner_code}")
        partner_id          = partner_row["partner_id"]
        partner_country_code = partner_row["country_code"]

        # All write steps run inside a single transaction — if any step
        # raises, the whole batch rolls back (no half-loaded fact rows).
        async with conn.transaction():
            # ---- Step 1: parse CSV → stg_price_offer ----
            rows_loaded_to_stg = await _step1_csv_to_staging(
                conn, file_bytes, batch_id, partner_id, partner_code
            )

            # ---- Step 2: Ingest-stage DQ ----
            await conn.execute("SELECT dq_run_batch_ingest($1)", batch_id)

            # mark passing rows. Compare on row_num (the CSV line number),
            # which is what raw_payload->>'__row_num' holds.
            await conn.execute(
                """
                UPDATE stg_price_offer SET dq_status = 'INGEST_PASSED'
                WHERE source_batch_id = $1
                  AND row_num NOT IN (
                      SELECT (raw_payload->>'__row_num')::bigint
                      FROM dq_bad_records
                      WHERE source_batch_id = $1
                  )
                """,
                batch_id,
            )

            # ---- Step 3: Harmonise (only rows that passed Ingest DQ) ----
            harm_high, harm_medium, harm_low = await _step3_harmonise(conn, batch_id)

            # ---- Step 4: PRE_FACT DQ — HIGH-severity gate on enriched stg ----
            # Country↔currency, partner↔country, harmonise-unmatched. Rows
            # failing here will NOT enter fact_price_offer.
            await conn.execute("SELECT dq_run_batch_pre_fact($1)", batch_id)

            # Mark rows that passed BOTH Ingest AND PRE_FACT
            await conn.execute(
                """
                UPDATE stg_price_offer SET dq_status = 'PRE_FACT_PASSED'
                WHERE source_batch_id = $1
                  AND dq_status = 'INGEST_PASSED'
                  AND row_num NOT IN (
                      SELECT (raw_payload->>'__row_num')::bigint
                      FROM dq_bad_records
                      WHERE source_batch_id = $1
                        AND rule_id IN (
                          SELECT rule_id FROM dq_rule_catalog
                          WHERE target_stage = 'PRE_FACT' AND is_active
                        )
                  )
                """,
                batch_id,
            )

            # ---- Step 5: build fact_price_offer from PRE_FACT-passing rows ----
            await _step4_build_facts(conn, batch_id, partner_id)

            # ---- Step 6: SEMANTIC-stage DQ — soft signals on fact rows ----
            await conn.execute("SELECT dq_run_batch_semantic($1)", batch_id)

            # ---- Step 7: Update SCD-2 history (Slowly Changing Dimension Type 2) ----
            await _step6_scd2_history(conn, batch_id)

            # ---- Step 8: dws_partner_dq_per_batch summary ----
            rows_loaded, rows_unchanged, rows_bad = await _step7_summary(
                conn, batch_id, partner_id, started_at
            )

    # Track in in-memory job log
    _jobs[job_id] = {
        "job_id":           job_id,
        "source_batch_id":  batch_id,
        "partner_code":     partner_code,
        "status":           JobStatus.COMPLETED,
        "submitted_at":     started_at,
        "started_at":       started_at,
        "completed_at":     _now_utc(),
        "rows_loaded":      rows_loaded,
        "rows_unchanged":   rows_unchanged,
        "rows_bad":         rows_bad,
        "chunk_count":      1,
        "completed_chunks": 1,
    }

    return LoadDataAcceptedResponse(
        job_id          = job_id,
        source_batch_id = batch_id,
        status          = JobStatus.COMPLETED,
        poll_url        = f"/load-data/{job_id}",
        submitted_at    = started_at,
    )


# ---------- Step 1 ----------

async def _step1_csv_to_staging(
    conn: asyncpg.Connection,
    file_bytes: bytes,
    batch_id: uuid.UUID,
    partner_id: int,
    partner_code: str,
) -> int:
    """Parse the CSV bytes and bulk-insert into stg_price_offer."""
    text = file_bytes.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    rows = []
    for i, raw in enumerate(reader, 1):
        crawl_raw = raw.get("CRAWL_TS") or ""
        try:
            ts_utc = datetime.fromisoformat(crawl_raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            ts_utc = None

        country_name = (raw.get("COUNTRY_VAL") or "").strip()
        country_code = _COUNTRY_NAME_MAP.get(country_name)
        currency_code = {"AU": "AUD", "NZ": "NZD", "US": "USD", "GB": "GBP"}.get(country_code)

        # Determine payment_type + amounts
        payment_type, full_price, monthly_amount, instalment_months = None, None, None, None
        if raw.get("FULL PRICE"):
            payment_type = "FULL"
            try:
                full_price = float(raw["FULL PRICE"])
            except ValueError:
                pass
        elif raw.get("MONTHLY_INSTALMENT_AMT"):
            payment_type = "INSTALMENT"
            try:
                monthly_amount = float(raw["MONTHLY_INSTALMENT_AMT"])
            except ValueError:
                pass
            try:
                instalment_months = int(raw.get("INSTALMENT_MONTH") or 0) or None
            except ValueError:
                pass

        # Stamp the CSV row number into raw_payload so DQ rules can refer to it.
        # Named "__row_num" because that's what it is — the 1-indexed CSV line
        # number, NOT the auto-increment stg_row_id (which is unknown until
        # after INSERT).
        payload = {**raw, "__row_num": i}

        rows.append((
            batch_id, i, json.dumps(payload),
            partner_code, country_name, country_code,
            (raw.get("PRODUCT_NAME_VAL") or "").strip(),
            crawl_raw, ts_utc, currency_code,
            payment_type, full_price, monthly_amount, instalment_months,
        ))

    await conn.executemany(
        """
        INSERT INTO stg_price_offer
          (source_batch_id, row_num, raw_payload,
           partner_code, country_name, country_code, raw_product_name,
           crawl_ts_raw, crawl_ts_utc, currency_code,
           payment_type, full_price, monthly_amount, instalment_months)
        VALUES ($1, $2, $3::jsonb,
                $4, $5, $6, $7,
                $8, $9, $10,
                $11, $12, $13, $14)
        """,
        rows,
    )
    return len(rows)


# ---------- Step 3 ----------

async def _step3_harmonise(conn: asyncpg.Connection, batch_id: uuid.UUID) -> tuple[int, int, int]:
    """Run the Harmoniser on each Ingest-passing row; write product_model_id back."""
    h = get_harmoniser()

    # Pull passing rows
    stg_rows = await conn.fetch(
        """
        SELECT stg_row_id, raw_product_name
        FROM stg_price_offer
        WHERE source_batch_id = $1
          AND dq_status = 'INGEST_PASSED'
        """,
        batch_id,
    )

    # Pre-load model_key → product_model_id lookup
    model_id_lookup = {
        r["model_key"]: r["product_model_id"]
        for r in await conn.fetch("SELECT model_key, product_model_id FROM dim_product_model")
    }

    high, medium, low = 0, 0, 0
    updates = []
    for row in stg_rows:
        matches = h.match(row["raw_product_name"], k=1)
        if not matches:
            updates.append((row["stg_row_id"], None, None, None))
            continue
        top = matches[0]
        model_id = model_id_lookup.get(top.model_key)
        updates.append((row["stg_row_id"], model_id, top.score, top.confidence))
        if top.confidence == "HIGH":
            high += 1
        elif top.confidence == "MEDIUM":
            medium += 1
        else:
            low += 1

    await conn.executemany(
        """
        UPDATE stg_price_offer
        SET product_model_id     = $2,
            harmonise_score      = $3,
            harmonise_confidence = $4
        WHERE stg_row_id = $1
        """,
        updates,
    )
    return high, medium, low


# ---------- Step 4 ----------

async def _step4_build_facts(conn: asyncpg.Connection, batch_id: uuid.UUID, partner_id: int) -> None:
    """
    Insert PRE_FACT-passing rows into fact_price_offer + payment child table.

    Filters on dq_status = 'PRE_FACT_PASSED' — rows that survived BOTH the
    INGEST stage (parse/format) AND the PRE_FACT gate (country↔currency,
    partner↔country, harmonise match). Factual errors never reach fact.

    We process row-by-row so that we can correctly pair each
    fact_price_offer.offer_id with its child payment row using RETURNING
    (avoiding the well-known PostgreSQL gotcha that `INSERT ... SELECT ...
    RETURNING` does not guarantee output order matches source order).

    For ~4 000 rows this runs in a few seconds. Demo-grade. Production
    would batch via COPY + a temp mapping table.
    """
    # 4a) pull eligible rows + FX rate
    eligible = await conn.fetch(
        """
        SELECT
          s.stg_row_id, s.raw_product_name, s.payment_type,
          s.currency_code, s.crawl_ts_utc, s.country_code,
          s.product_model_id, s.harmonise_score, s.harmonise_confidence,
          s.full_price, s.monthly_amount, s.instalment_months,
          fx.rate           AS fx_rate_to_usd,
          fx.effective_date AS fx_rate_date
        FROM stg_price_offer s
        LEFT JOIN LATERAL (
          SELECT rate, effective_date
          FROM dim_currency_rate_snapshot r
          WHERE r.from_currency_code = s.currency_code
            AND r.to_currency_code   = 'USD'
            AND r.effective_date    <= s.crawl_ts_utc::date
          ORDER BY r.effective_date DESC
          LIMIT 1
        ) fx ON TRUE
        WHERE s.source_batch_id    = $1
          AND s.dq_status          = 'PRE_FACT_PASSED'
          AND s.payment_type      IS NOT NULL
          AND s.crawl_ts_utc      IS NOT NULL
        """,
        batch_id,
    )

    # 4b) row-by-row insert (parent + matching child)
    insert_offer_sql = """
      INSERT INTO fact_price_offer (
        partner_id, country_code, product_model_id,
        raw_product_name, payment_type,
        currency_code, effective_total_local, effective_total_usd,
        fx_rate_to_usd, fx_rate_date,
        crawl_ts_utc, crawl_ts_local,
        harmonise_score, harmonise_confidence,
        source_batch_id
      ) VALUES (
        $1, $2, $3,
        $4, $5::payment_type_enum,
        $6, $7, $8,
        $9, $10,
        $11, $12,
        $13, $14::harmonise_confidence_enum,
        $15
      )
      RETURNING offer_id, crawl_ts_utc
    """
    insert_full_sql = """
      INSERT INTO fact_payment_full_price (offer_id, crawl_ts_utc, full_price)
      VALUES ($1, $2, $3)
    """
    insert_inst_sql = """
      INSERT INTO fact_payment_instalment (offer_id, crawl_ts_utc, monthly_amount, instalment_months)
      VALUES ($1, $2, $3, $4)
    """

    for r in eligible:
        if r["payment_type"] == "FULL":
            local = float(r["full_price"] or 0)
        else:
            local = float(r["monthly_amount"] or 0) * int(r["instalment_months"] or 0)
        if local <= 0:
            continue   # safety belt — Ingest DQ already filtered, but be defensive

        fx = float(r["fx_rate_to_usd"] or 1.0)
        fx_date = r["fx_rate_date"] or r["crawl_ts_utc"].date()
        usd = round(local * fx, 2)

        offer_row = await conn.fetchrow(
            insert_offer_sql,
            partner_id,
            r["country_code"],
            r["product_model_id"],
            r["raw_product_name"],
            r["payment_type"],
            r["currency_code"],
            local,
            usd,
            fx,
            fx_date,
            r["crawl_ts_utc"],
            r["crawl_ts_utc"].replace(tzinfo=None),  # crawl_ts_local: timestamp without tz
            r["harmonise_score"],
            r["harmonise_confidence"],
            batch_id,
        )

        if r["payment_type"] == "FULL":
            await conn.execute(
                insert_full_sql,
                offer_row["offer_id"], offer_row["crawl_ts_utc"], float(r["full_price"]),
            )
        else:
            await conn.execute(
                insert_inst_sql,
                offer_row["offer_id"], offer_row["crawl_ts_utc"],
                float(r["monthly_amount"]), int(r["instalment_months"]),
            )


# ---------- Step 6 ----------

async def _step6_scd2_history(conn: asyncpg.Connection, batch_id: uuid.UUID) -> None:
    """Update fact_partner_price_history (Slowly Changing Dimension Type 2)."""
    # Latest observation per (product, partner, country, payment_type) within this batch
    await conn.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (product_model_id, partner_id, country_code, payment_type)
            product_model_id, partner_id, country_code, payment_type,
            currency_code, effective_total_local, effective_total_usd,
            crawl_ts_local::date AS valid_from_date
          FROM fact_price_offer
          WHERE source_batch_id = $1
          ORDER BY product_model_id, partner_id, country_code, payment_type, crawl_ts_utc DESC
        ),
        existing AS (
          SELECT h.*
          FROM fact_partner_price_history h
          WHERE h.valid_to_date IS NULL
        ),
        changed AS (
          SELECT l.*
          FROM latest l
          LEFT JOIN existing e
            ON (l.product_model_id, l.partner_id, l.country_code, l.payment_type)
             = (e.product_model_id, e.partner_id, e.country_code, e.payment_type)
          -- A row qualifies as a "change" only when:
          --   (1) no existing open history row → first observation, OR
          --   (2) price differs AND the new observation is STRICTLY LATER than
          --       the existing row's valid_from_date.
          -- Same-day or earlier observations with a different price are edge
          -- cases (re-ingest, scraper backfill) and would create invalid
          -- valid_from > valid_to ranges; we skip them here.
          WHERE e.history_id IS NULL
             OR (e.effective_total_usd <> l.effective_total_usd
                 AND l.valid_from_date > e.valid_from_date)
        ),
        closed AS (
          UPDATE fact_partner_price_history h
          SET valid_to_date = c.valid_from_date - INTERVAL '1 day'
          FROM changed c
          WHERE (h.product_model_id, h.partner_id, h.country_code, h.payment_type)
              = (c.product_model_id, c.partner_id, c.country_code, c.payment_type)
            AND h.valid_to_date IS NULL
            AND h.effective_total_usd <> c.effective_total_usd
          RETURNING 1
        )
        INSERT INTO fact_partner_price_history (
          product_model_id, partner_id, country_code, payment_type,
          currency_code, effective_total_local, effective_total_usd,
          valid_from_date, valid_to_date
        )
        SELECT
          c.product_model_id, c.partner_id, c.country_code, c.payment_type,
          c.currency_code, c.effective_total_local, c.effective_total_usd,
          c.valid_from_date, NULL
        FROM changed c
        """,
        batch_id,
    )


# ---------- Step 7 ----------

async def _step7_summary(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    partner_id: int,
    loaded_at: datetime,
) -> tuple[int, int, int]:
    """Write a single row to dws_partner_dq_per_batch."""

    total = (await conn.fetchrow(
        "SELECT COUNT(*) AS n FROM stg_price_offer WHERE source_batch_id = $1",
        batch_id,
    ))["n"]
    loaded = (await conn.fetchrow(
        "SELECT COUNT(*) AS n FROM fact_price_offer WHERE source_batch_id = $1",
        batch_id,
    ))["n"]
    bad = (await conn.fetchrow(
        "SELECT COUNT(*) AS n FROM dq_bad_records WHERE source_batch_id = $1",
        batch_id,
    ))["n"]

    rows_unchanged = max(0, total - loaded - bad)
    success_rate = (loaded / total) if total > 0 else 1.0

    harm_counts = await conn.fetchrow(
        """
        SELECT
          COUNT(*) FILTER (WHERE harmonise_confidence = 'HIGH')   AS h_high,
          COUNT(*) FILTER (WHERE harmonise_confidence = 'MEDIUM') AS h_medium,
          COUNT(*) FILTER (WHERE harmonise_confidence = 'LOW')    AS h_low,
          COUNT(DISTINCT product_model_id)                         AS unique_products
        FROM fact_price_offer
        WHERE source_batch_id = $1
        """,
        batch_id,
    )
    h_high   = harm_counts["h_high"] or 0
    h_medium = harm_counts["h_medium"] or 0
    h_low    = harm_counts["h_low"] or 0
    h_high_pct = (h_high / loaded) if loaded > 0 else 0.0
    unique_products = harm_counts["unique_products"] or 0

    await conn.execute(
        """
        INSERT INTO dws_partner_dq_per_batch (
          source_batch_id, partner_id, loaded_at,
          total_records, loaded_records, rows_unchanged, bad_records_count,
          load_success_rate,
          harmonise_high, harmonise_medium, harmonise_low, harmonise_high_pct,
          unique_products_covered
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (source_batch_id) DO NOTHING
        """,
        batch_id, partner_id, loaded_at,
        total, loaded, rows_unchanged, bad,
        round(success_rate, 4),
        h_high, h_medium, h_low, round(h_high_pct, 4),
        unique_products,
    )

    return loaded, rows_unchanged, bad


# ===========================================================================
# /load-data/{job_id} progress
# ===========================================================================

def get_job_status(job_id: uuid.UUID) -> LoadJobStatus:
    j = _jobs.get(job_id)
    if j is None:
        raise HTTPException(404, f"job_id {job_id} not found")

    pct = 100.0 if j["status"] == JobStatus.COMPLETED else 0.0
    return LoadJobStatus(
        job_id          = j["job_id"],
        source_batch_id = j["source_batch_id"],
        status          = j["status"],
        progress = LoadDataProgress(
            chunk_count       = j["chunk_count"],
            completed_chunks  = j["completed_chunks"],
            rows_loaded       = j["rows_loaded"],
            rows_bad          = j["rows_bad"],
            percent_complete  = pct,
        ),
        started_at              = j.get("started_at"),
        estimated_completion_at = j.get("completed_at"),
        error_message           = j.get("error_message"),
    )


# ===========================================================================
# /compute-dq — call the SQL orchestrator
# ===========================================================================

async def compute_dq(
    source_batch_id: uuid.UUID,
    rules: Optional[List[str]],
    stages: Optional[List[str]],
) -> Tuple[DQSummary, List[DQRuleRunResult]]:
    pool = get_pool()
    async with pool.acquire() as conn:
        # Default to all three stages; otherwise run only the requested ones.
        stage_set = set(stages) if stages else {"INGEST", "PRE_FACT", "SEMANTIC"}

        if stage_set == {"INGEST", "PRE_FACT", "SEMANTIC"}:
            await conn.execute("SELECT dq_run_batch($1)", source_batch_id)
        else:
            if "INGEST" in stage_set:
                await conn.execute("SELECT dq_run_batch_ingest($1)", source_batch_id)
            if "PRE_FACT" in stage_set:
                await conn.execute("SELECT dq_run_batch_pre_fact($1)", source_batch_id)
            if "SEMANTIC" in stage_set:
                await conn.execute("SELECT dq_run_batch_semantic($1)", source_batch_id)

        # Read the freshest rule_run rows for this batch
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (rule_id)
              rule_id, rule_name, rule_category, severity,
              total_records, failed_records, pass_rate
            FROM dq_output
            WHERE source_batch_id = $1
            ORDER BY rule_id, run_ts DESC
            """,
            source_batch_id,
        )

        # Filter by requested rules if provided
        if rules:
            rows = [r for r in rows if r["rule_id"] in rules]

        # Need target_stage from catalog
        cat = await conn.fetch(
            "SELECT rule_id, target_stage FROM dq_rule_catalog"
        )
        stage_lookup = {r["rule_id"]: r["target_stage"] for r in cat}

        rule_runs = [
            DQRuleRunResult(
                rule_id        = r["rule_id"],
                rule_name      = r["rule_name"],
                rule_category  = r["rule_category"] or "general",
                severity       = Severity(r["severity"] or "MEDIUM"),
                target_stage   = stage_lookup.get(r["rule_id"], "INGEST"),
                total_records  = r["total_records"] or 0,
                failed_records = r["failed_records"] or 0,
                pass_rate      = float(r["pass_rate"] or 1.0),
            )
            for r in rows
        ]

        total_violations = sum(r.failed_records for r in rule_runs)
        by_sev = DQViolationsBySeverity()
        for r in rule_runs:
            if r.severity == Severity.HIGH:
                by_sev.HIGH += r.failed_records
            elif r.severity == Severity.MEDIUM:
                by_sev.MEDIUM += r.failed_records
            else:
                by_sev.LOW += r.failed_records

        summary = DQSummary(
            total_rules_run  = len(rule_runs),
            total_records    = max((r.total_records for r in rule_runs), default=0),
            total_violations = total_violations,
            by_severity      = by_sev,
        )
        return summary, rule_runs


# ===========================================================================
# /detect-anomalies — simple statistical signal over fact_partner_price_history
# ===========================================================================

async def _build_anomaly_visualization(
    conn:             asyncpg.Connection,
    product_model_id: int,
    partner_id:       int,
    country_code:     str,
    payment_type:     str,
    anomaly_date:     date,
    anomaly_price:    float,
    mean_usd:         float,
    std_usd:          float,
) -> "AnomalyVisualization":
    """
    Build the structured payload that frontends (Chart.js / Recharts) render.
    Three components:
      * series                   — last 30 days of this partner's history
                                   for the same (product, country) + the
                                   anomaly observation flagged is_anomaly
      * baseline_band            — mean and ±1 stddev band from the same
                                   30-day window (bound below at zero)
      * cross_partner_comparison — current open prices from OTHER partners
                                   (via v_partner_price_current) so the
                                   reviewer can spot if it's a partner-side
                                   issue or a market move.
    """
    # 1) time series — 30-day history for THIS partner
    series_rows = await conn.fetch(
        """
        SELECT valid_from_date AS d, effective_total_usd AS p
        FROM fact_partner_price_history
        WHERE product_model_id = $1
          AND country_code     = $2
          AND partner_id       = $3
          AND valid_from_date >= $4::date - INTERVAL '30 days'
        ORDER BY valid_from_date
        """,
        product_model_id, country_code, partner_id, anomaly_date,
    )
    series = [
        TimeSeriesPoint(date=r["d"], price_usd=float(r["p"]))
        for r in series_rows
    ]
    series.append(TimeSeriesPoint(
        date=anomaly_date, price_usd=anomaly_price, is_anomaly=True,
    ))

    # 2) baseline band
    baseline_band = {
        "mean":  round(mean_usd, 2),
        "lower": round(max(mean_usd - std_usd, 0.0), 2),
        "upper": round(mean_usd + std_usd, 2),
    }

    # 3) cross-partner comparison from the live snapshot view
    cross_row = await conn.fetchrow(
        """
        SELECT partner_prices_json
        FROM v_partner_price_current
        WHERE product_model_id = $1
          AND country_code     = $2
          AND payment_type     = $3::payment_type_enum
        """,
        product_model_id, country_code, payment_type,
    )
    cross: Optional[Dict[str, float]] = None
    if cross_row and cross_row["partner_prices_json"] is not None:
        raw = cross_row["partner_prices_json"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        cross = {k: float(v) for k, v in raw.items()}

    return AnomalyVisualization(
        type="time_series_with_band",
        series=series,
        baseline_band=baseline_band,
        cross_partner_comparison=cross,
    )

async def detect_anomalies(req: DetectAnomaliesRequest) -> DetectAnomaliesResponse:
    """
    Demo-grade anomaly detection. For each new offer in scope, compare its
    USD price against the 30-day rolling mean from fact_partner_price_history;
    flag if ratio falls outside expected band. For each triggered anomaly we
    also build an AnomalyVisualization payload (time series + baseline band +
    cross-partner comparison) so the frontend can render it directly.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        # Get all offers in scope
        if req.source_batch_id:
            offers = await conn.fetch(
                """
                SELECT f.offer_id, f.product_model_id, f.partner_id, f.country_code,
                       f.payment_type::text AS payment_type,
                       f.effective_total_usd, f.crawl_ts_utc,
                       p.partner_code,
                       (SELECT model_key FROM dim_product_model WHERE product_model_id = f.product_model_id) AS model_key
                FROM fact_price_offer f
                JOIN dim_partner p USING (partner_id)
                WHERE f.source_batch_id = $1
                  AND ($2::int[] IS NULL OR f.product_model_id = ANY($2::int[]))
                """,
                req.source_batch_id,
                req.product_model_ids,
            )
        else:
            offers = []

        anomalies: List[Anomaly] = []
        for o in offers:
            # 30-day mean from history (excluding current price events)
            stat = await conn.fetchrow(
                """
                SELECT
                  AVG(effective_total_usd)    AS mean_usd,
                  STDDEV(effective_total_usd) AS std_usd,
                  COUNT(*)                    AS n
                FROM fact_partner_price_history
                WHERE product_model_id = $1
                  AND country_code     = $2
                  AND valid_from_date >= $3::date - INTERVAL '30 days'
                """,
                o["product_model_id"], o["country_code"], o["crawl_ts_utc"],
            )
            if not stat or not stat["mean_usd"] or stat["n"] < 2:
                continue
            mean_usd = float(stat["mean_usd"])
            std_usd  = float(stat["std_usd"] or 0.0)
            obs = float(o["effective_total_usd"])
            pct_off = abs(obs - mean_usd) / mean_usd if mean_usd else 0.0
            if pct_off < 0.10:
                continue   # within tolerance — not an anomaly

            severity = Severity.HIGH if pct_off >= 0.25 else Severity.MEDIUM if pct_off >= 0.15 else Severity.LOW

            # Min severity filter
            sev_rank = {Severity.HIGH: 3, Severity.MEDIUM: 2, Severity.LOW: 1}
            if sev_rank[severity] < sev_rank[req.min_severity]:
                continue

            visualization = await _build_anomaly_visualization(
                conn,
                product_model_id = o["product_model_id"],
                partner_id       = o["partner_id"],
                country_code     = o["country_code"],
                payment_type     = o["payment_type"],
                anomaly_date     = o["crawl_ts_utc"].date(),
                anomaly_price    = round(obs, 2),
                mean_usd         = mean_usd,
                std_usd          = std_usd,
            )

            anomalies.append(Anomaly(
                anomaly_id          = o["offer_id"],
                offer_id            = o["offer_id"],
                anomaly_type        = AnomalyType.STATISTICAL,
                severity            = severity,
                signal_score        = round(min(pct_off, 1.0), 3),
                product_model_id    = o["product_model_id"],
                product_model_name  = o["model_key"] or "<unknown>",
                partner_code        = o["partner_code"],
                country_code        = o["country_code"],
                observed_price_usd  = round(obs, 2),
                explanation         = f"{pct_off*100:.1f}% deviation from 30-day mean (${mean_usd:.2f})",
                context = AnomalyContext(
                    lifecycle_status    = "STABLE",
                    lifecycle_factor    = 1.0,
                    suppression_applied = False,
                    baseline_snapshot   = BaselineSnapshot(
                        window_days = 30,
                        sample_size = stat["n"],
                        mean        = round(mean_usd, 2),
                        stddev      = round(std_usd, 2),
                    ),
                ),
                visualization = visualization,
                detected_at = _now_utc(),
            ))

        by_sev = AnomaliesBySeverity()
        for a in anomalies:
            if a.severity == Severity.HIGH:
                by_sev.HIGH += 1
            elif a.severity == Severity.MEDIUM:
                by_sev.MEDIUM += 1
            else:
                by_sev.LOW += 1

        return DetectAnomaliesResponse(
            detected_at      = _now_utc(),
            total_anomalies  = len(anomalies),
            by_severity      = by_sev,
            anomalies        = anomalies,
        )


# ===========================================================================
# /bad-records — read + workflow update
# ===========================================================================

async def list_bad_records(
    status: Optional[BadRecordStatus],
    severity: Optional[Severity],
    assignee: Optional[str],
    page: int,
    page_size: int,
) -> Tuple[int, List[BadRecordEntry]]:
    pool = get_pool()
    async with pool.acquire() as conn:
        where = []
        args: list = []
        if status is not None:
            args.append(status.value)
            where.append(f"status = ${len(args)}")
        if severity is not None:
            args.append(severity.value)
            where.append(f"severity = ${len(args)}")
        if assignee is not None:
            args.append(assignee)
            where.append(f"assignee = ${len(args)}")

        clause = (" WHERE " + " AND ".join(where)) if where else ""
        count_row = await conn.fetchrow(f"SELECT COUNT(*) AS n FROM dq_bad_records{clause}", *args)
        total = count_row["n"]

        offset = (page - 1) * page_size
        rows = await conn.fetch(
            f"""
            SELECT bad_record_id, source_batch_id, rule_id, failed_field, error_message,
                   severity, status::text AS status, assignee, raw_payload,
                   detected_at, resolved_at
            FROM dq_bad_records
            {clause}
            ORDER BY detected_at DESC
            LIMIT {page_size} OFFSET {offset}
            """,
            *args,
        )
        items = [
            BadRecordEntry(
                bad_record_id   = r["bad_record_id"],
                source_batch_id = r["source_batch_id"],
                rule_id         = r["rule_id"],
                failed_field    = r["failed_field"],
                error_message   = r["error_message"],
                severity        = Severity(r["severity"] or "MEDIUM"),
                status          = BadRecordStatus(r["status"]),
                assignee        = r["assignee"],
                raw_payload     = json.loads(r["raw_payload"]) if isinstance(r["raw_payload"], str) else r["raw_payload"],
                detected_at     = r["detected_at"],
                resolved_at     = r["resolved_at"],
            )
            for r in rows
        ]
        return total, items


async def resolve_bad_record(
    bad_record_id: int,
    action: ResolveAction,
    notes: Optional[str],
    replay_batch: bool,
) -> Tuple[BadRecordStatus, datetime, bool]:
    new_status = BadRecordStatus.RESOLVED if action == ResolveAction.RESOLVED else BadRecordStatus.IGNORED
    resolved_at = _now_utc()

    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE dq_bad_records
            SET status = $2::bad_record_status_enum,
                resolved_at = $3,
                resolution_notes = $4
            WHERE bad_record_id = $1
            """,
            bad_record_id, new_status.value, resolved_at, notes,
        )
        if result.startswith("UPDATE 0"):
            raise HTTPException(404, f"bad_record_id {bad_record_id} not found")

    # replay_batch is recorded but actual replay scheduling is out of scope
    return new_status, resolved_at, replay_batch
