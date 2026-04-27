"""
Service layer — bridges API endpoints to PostgreSQL + the harmonise/dq modules.

Architecture:
  9 internal step helpers (parse → ingest_dq → harmonise → prefact_dq →
  write_fact → semantic_dq → scd2 → detect_anomalies → batch_summary), each
  acting on an asyncpg.Connection so callers can compose them in a single
  transaction. Two call paths share the same helpers:

  Path A — POST /pipeline           (orchestrator, full 9-step interleaved
                                     order, PRE_FACT gate hard-blocks)
  Path B — POST /load-data
         + POST /compute-dq         (sub-modules, grouped by responsibility,
         + POST /detect-anomalies    PRE_FACT degrades to post-hoc flag)
         + GET  /harmonise-product

  Both paths call the same 9 helpers and write to the same tables. Path B
  exists because Task B requires 4 independently callable endpoints; Path A
  exists for one-click end-to-end execution with strict gating semantics.
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
    LoadJobStatus, PipelineResponse, ResolveAction, Severity, SignalBreakdown,
    TimeSeriesPoint,
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
    """GET /harmonise-product — single-product harmonise (algorithmic, in-process)."""
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
# Step constants — country/currency lookup
# ===========================================================================

# CSV column → standardised partner_code
_PARTNER_CODE_MAP = {"Partner A": "PARTNER_A", "Partner B": "PARTNER_B"}

# country_name → ISO code. Accepts BOTH the full English name AND the ISO
# 3166-1 alpha-2 code, because real partner feeds are inconsistent — Partner B
# sample data has 154 rows where COUNTRY_VAL = "NZ" instead of "New Zealand".
# Both are legitimate representations of the same country, so DQ should NOT
# flag them as errors.
_COUNTRY_NAME_MAP = {
    "Australia":      "AU",
    "New Zealand":    "NZ",
    "United States":  "US",
    "United Kingdom": "GB",
    "AU": "AU",
    "NZ": "NZ",
    "US": "US",
    "GB": "GB",
}


# ===========================================================================
# 9 Step Helpers — internal building blocks shared by Path A (/pipeline)
# and Path B (sub-module endpoints). Each accepts an asyncpg.Connection so
# callers control transactions.
# ===========================================================================

# ---------- Step 1: parse CSV → stg_price_offer ----------

async def parse_csv_to_stg(
    conn: asyncpg.Connection,
    file_bytes: bytes,
    batch_id: uuid.UUID,
    partner_id: int,
    partner_code: str,
) -> int:
    """Parse CSV bytes and bulk-insert into stg_price_offer. Returns row count."""
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

        # __row_num = 1-indexed CSV line number (NOT the auto-increment stg_row_id,
        # which is unknown until INSERT). DQ rules reference this via raw_payload.
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


# ---------- Step 2: INGEST DQ ----------

async def run_ingest_dq(conn: asyncpg.Connection, batch_id: uuid.UUID) -> None:
    """Run INGEST-stage DQ rules on stg_price_offer; mark passing rows."""
    await conn.execute("SELECT dq_run_batch_ingest($1)", batch_id)
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


# ---------- Step 3: harmonise rows in stg ----------

async def harmonise_in_stg(conn: asyncpg.Connection, batch_id: uuid.UUID) -> Tuple[int, int, int]:
    """Run the Harmoniser on each row; write product_model_id back to stg.

    Runs on every row in the batch with a raw_product_name. dq_status is
    intentionally NOT filtered — Path A (with prior INGEST DQ) and Path B
    (without prior DQ) both harmonise the whole batch. Path A's INGEST-failing
    rows are still harmonised (negligible cost, prevents missing matches when
    a row fails INGEST for an unrelated reason like NULL crawl_ts).
    Returns (high, medium, low) confidence counts.
    """
    h = get_harmoniser()

    stg_rows = await conn.fetch(
        """
        SELECT stg_row_id, raw_product_name
        FROM stg_price_offer
        WHERE source_batch_id = $1
          AND raw_product_name IS NOT NULL
          AND raw_product_name <> ''
        """,
        batch_id,
    )

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


# ---------- Step 4: PRE_FACT DQ ----------

async def run_prefact_dq(conn: asyncpg.Connection, batch_id: uuid.UUID) -> None:
    """Run PRE_FACT-stage DQ rules on stg+harmonise data; mark passing rows.

    Only rows currently marked INGEST_PASSED can be promoted to PRE_FACT_PASSED
    — PENDING rows are by definition INGEST-failing and must not advance.
    """
    await conn.execute("SELECT dq_run_batch_pre_fact($1)", batch_id)
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


# ---------- Step 5: write stg → fact_price_offer ----------

async def write_stg_to_fact(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    partner_id: int,
    *,
    gate: bool,
) -> int:
    """Insert stg rows into fact_price_offer + payment child tables.

    gate=True  (Path A /pipeline): only PRE_FACT_PASSED rows enter fact.
                                   Bad rows are blocked at the boundary.
    gate=False (Path B /load-data): all rows with parseable fields enter fact.
                                    Bad rows are flagged post-hoc by /compute-dq
                                    in dq_bad_records but stay in fact.

    Row-by-row insert pairs each fact_price_offer.offer_id with its child
    payment row via RETURNING (avoiding the PostgreSQL gotcha that
    `INSERT ... SELECT ... RETURNING` does not guarantee output order).
    """
    if gate:
        where_clause = "AND s.dq_status = 'PRE_FACT_PASSED'"
    else:
        # Path B has no DQ run yet; require parseable payment + crawl ts only
        where_clause = ""

    eligible = await conn.fetch(
        f"""
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
          {where_clause}
          AND s.payment_type      IS NOT NULL
          AND s.crawl_ts_utc      IS NOT NULL
          AND s.country_code      IS NOT NULL
          AND s.product_model_id  IS NOT NULL
        """,
        batch_id,
    )

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

    inserted = 0
    for r in eligible:
        if r["payment_type"] == "FULL":
            local = float(r["full_price"] or 0)
        else:
            local = float(r["monthly_amount"] or 0) * int(r["instalment_months"] or 0)
        if local <= 0:
            continue

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
            r["crawl_ts_utc"].replace(tzinfo=None),
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
        inserted += 1
    return inserted


# ---------- Step 6: SEMANTIC DQ ----------

async def run_semantic_dq(conn: asyncpg.Connection, batch_id: uuid.UUID) -> None:
    """Run SEMANTIC-stage DQ rules on fact rows (soft signals; flag-and-keep)."""
    await conn.execute("SELECT dq_run_batch_semantic($1)", batch_id)


# ---------- Step 7: SCD-2 history ----------

async def update_scd2(conn: asyncpg.Connection, batch_id: uuid.UUID) -> None:
    """Update fact_partner_price_history (Slowly Changing Dimension Type 2)."""
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


# ---------- Step 8: detect anomalies for a batch ----------

async def detect_anomalies_for_batch(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    min_severity: Severity = Severity.LOW,
    product_model_ids: Optional[List[int]] = None,
) -> List[Anomaly]:
    """Detect pricing anomalies on the fact rows belonging to this batch.

    For each offer in scope, compares its USD price against the 30-day rolling
    mean from fact_partner_price_history. Builds a visualization payload
    (time series + baseline band + cross-partner comparison) for each anomaly
    so the frontend can render it directly.
    """
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
        batch_id,
        product_model_ids,
    )

    anomalies: List[Anomaly] = []
    sev_rank = {Severity.HIGH: 3, Severity.MEDIUM: 2, Severity.LOW: 1}

    for o in offers:
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
            continue

        severity = (Severity.HIGH if pct_off >= 0.25
                    else Severity.MEDIUM if pct_off >= 0.15
                    else Severity.LOW)

        if sev_rank[severity] < sev_rank[min_severity]:
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
    return anomalies


# ---------- Step 9: write batch summary ----------

async def write_batch_summary(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    partner_id: int,
    loaded_at: datetime,
) -> Tuple[int, int, int]:
    """Upsert dws_partner_dq_per_batch row.

    Idempotent — recomputes all columns from current DB state on each call.
    Path A: called once at end of /pipeline. Path B: each sub-module calls
    it at its tail; the row is incrementally refined as more steps complete.
    Returns (loaded_records, rows_unchanged, bad_records_count).
    """
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
        ON CONFLICT (source_batch_id) DO UPDATE SET
          total_records           = EXCLUDED.total_records,
          loaded_records          = EXCLUDED.loaded_records,
          rows_unchanged          = EXCLUDED.rows_unchanged,
          bad_records_count       = EXCLUDED.bad_records_count,
          load_success_rate       = EXCLUDED.load_success_rate,
          harmonise_high          = EXCLUDED.harmonise_high,
          harmonise_medium        = EXCLUDED.harmonise_medium,
          harmonise_low           = EXCLUDED.harmonise_low,
          harmonise_high_pct      = EXCLUDED.harmonise_high_pct,
          unique_products_covered = EXCLUDED.unique_products_covered
        """,
        batch_id, partner_id, loaded_at,
        total, loaded, rows_unchanged, bad,
        round(success_rate, 4),
        h_high, h_medium, h_low, round(h_high_pct, 4),
        unique_products,
    )
    return loaded, rows_unchanged, bad


# ---------- Anomaly visualization helper ----------

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
) -> AnomalyVisualization:
    """Build the structured payload that frontends (Chart.js / Recharts) render."""
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

    baseline_band = {
        "mean":  round(mean_usd, 2),
        "lower": round(max(mean_usd - std_usd, 0.0), 2),
        "upper": round(mean_usd + std_usd, 2),
    }

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


# ===========================================================================
# Service entry points — handle pool/transaction, call helpers, shape response
# ===========================================================================

async def _resolve_partner(conn: asyncpg.Connection, partner_code: str) -> int:
    row = await conn.fetchrow(
        "SELECT partner_id FROM dim_partner WHERE partner_code = $1",
        partner_code,
    )
    if not row:
        raise HTTPException(400, f"Unknown partner_code: {partner_code}")
    return row["partner_id"]


# ---------- POST /load-data — Path B sub-module ----------

async def submit_load_job(file_bytes: bytes, partner_code: str) -> LoadDataAcceptedResponse:
    """Path B /load-data sub-module.

    Steps covered: 1 (parse), 3 (harmonise), 5 (write fact, NO gate), 7 (SCD-2),
    9 (summary). DQ steps 2/4/6 are NOT run here — the caller must invoke
    /compute-dq separately. The PRE_FACT gate is NOT applied (gate=False);
    bad rows enter fact and must be filtered post-hoc via dq_bad_records.
    """
    job_id   = uuid.uuid4()
    batch_id = uuid.uuid4()
    started_at = _now_utc()

    pool = get_pool()
    async with pool.acquire() as conn:
        partner_id = await _resolve_partner(conn, partner_code)
        async with conn.transaction():
            await parse_csv_to_stg(conn, file_bytes, batch_id, partner_id, partner_code)
            await harmonise_in_stg(conn, batch_id)
            await write_stg_to_fact(conn, batch_id, partner_id, gate=False)
            await update_scd2(conn, batch_id)
            rows_loaded, rows_unchanged, rows_bad = await write_batch_summary(
                conn, batch_id, partner_id, started_at,
            )

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


# ---------- POST /compute-dq — Path B sub-module ----------

async def compute_dq_service(
    source_batch_id: uuid.UUID,
) -> Tuple[DQSummary, List[DQRuleRunResult]]:
    """Path B /compute-dq sub-module.

    Steps covered: 2 (INGEST DQ), 4 (PRE_FACT DQ), 6 (SEMANTIC DQ), 9 (summary).
    All 13 rules execute against the batch and write to dq_output + dq_bad_records.
    Called after /load-data; bad rows are flagged post-hoc (no gate).
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        # Resolve partner_id from any fact/stg row in this batch (for summary)
        partner_id = None
        row = await conn.fetchrow(
            "SELECT partner_id FROM fact_price_offer WHERE source_batch_id = $1 LIMIT 1",
            source_batch_id,
        )
        if row:
            partner_id = row["partner_id"]
        else:
            row = await conn.fetchrow(
                """
                SELECT p.partner_id
                FROM stg_price_offer s
                JOIN dim_partner p ON p.partner_code = s.partner_code
                WHERE s.source_batch_id = $1
                LIMIT 1
                """,
                source_batch_id,
            )
            if row:
                partner_id = row["partner_id"]

        async with conn.transaction():
            await run_ingest_dq(conn, source_batch_id)
            await run_prefact_dq(conn, source_batch_id)
            await run_semantic_dq(conn, source_batch_id)
            if partner_id is not None:
                await write_batch_summary(conn, source_batch_id, partner_id, _now_utc())

        # Read latest per-rule results
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
        cat = await conn.fetch("SELECT rule_id, target_stage FROM dq_rule_catalog")
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


# ---------- POST /detect-anomalies — Path B sub-module ----------

async def detect_anomalies_service(req: DetectAnomaliesRequest) -> DetectAnomaliesResponse:
    """Path B /detect-anomalies sub-module.

    Step covered: 8 (anomaly detection). Reads fact_price_offer + 30-day
    fact_partner_price_history baseline. Also refreshes step 9 summary.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        if not req.source_batch_id:
            return DetectAnomaliesResponse(
                detected_at      = _now_utc(),
                total_anomalies  = 0,
                by_severity      = AnomaliesBySeverity(),
                anomalies        = [],
            )
        anomalies = await detect_anomalies_for_batch(
            conn,
            batch_id          = req.source_batch_id,
            min_severity      = req.min_severity,
            product_model_ids = req.product_model_ids,
        )

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


# ---------- POST /pipeline — Path A orchestrator ----------

async def run_pipeline(file_bytes: bytes, partner_code: str) -> PipelineResponse:
    """Path A /pipeline orchestrator — runs all 9 steps in interleaved order.

    Sequence: 1→2→3→4→5(gate)→6→7→8→9.
    PRE_FACT gate hard-blocks bad rows from entering fact_price_offer.
    Single transaction; on any failure the entire batch rolls back.
    """
    job_id   = uuid.uuid4()
    batch_id = uuid.uuid4()
    started_at = _now_utc()

    pool = get_pool()
    async with pool.acquire() as conn:
        partner_id = await _resolve_partner(conn, partner_code)

        async with conn.transaction():
            await parse_csv_to_stg(conn, file_bytes, batch_id, partner_id, partner_code)  # 1
            await run_ingest_dq(conn, batch_id)                                            # 2
            await harmonise_in_stg(conn, batch_id)                                         # 3
            await run_prefact_dq(conn, batch_id)                                           # 4
            await write_stg_to_fact(conn, batch_id, partner_id, gate=True)                 # 5
            await run_semantic_dq(conn, batch_id)                                          # 6
            await update_scd2(conn, batch_id)                                              # 7
            anomalies = await detect_anomalies_for_batch(conn, batch_id)                   # 8
            rows_loaded, rows_unchanged, rows_bad = await write_batch_summary(             # 9
                conn, batch_id, partner_id, started_at,
            )

        # DQ summary (read after transaction commits)
        dq_rows = await conn.fetch(
            """
            SELECT severity, SUM(failed_records) AS fail
            FROM dq_output
            WHERE source_batch_id = $1
            GROUP BY severity
            """,
            batch_id,
        )
        dq_by_sev = DQViolationsBySeverity()
        for r in dq_rows:
            sev = (r["severity"] or "MEDIUM").upper()
            n = int(r["fail"] or 0)
            if sev == "HIGH":   dq_by_sev.HIGH = n
            elif sev == "LOW":  dq_by_sev.LOW = n
            else:               dq_by_sev.MEDIUM = n
        total_records = (await conn.fetchrow(
            "SELECT MAX(total_records) AS n FROM dq_output WHERE source_batch_id = $1",
            batch_id,
        ))["n"] or 0
        rules_run = (await conn.fetchrow(
            "SELECT COUNT(DISTINCT rule_id) AS n FROM dq_output WHERE source_batch_id = $1",
            batch_id,
        ))["n"] or 0
        dq_summary = DQSummary(
            total_rules_run  = rules_run,
            total_records    = total_records,
            total_violations = dq_by_sev.HIGH + dq_by_sev.MEDIUM + dq_by_sev.LOW,
            by_severity      = dq_by_sev,
        )

    completed_at = _now_utc()
    by_sev = AnomaliesBySeverity()
    for a in anomalies:
        if a.severity == Severity.HIGH:   by_sev.HIGH += 1
        elif a.severity == Severity.MEDIUM: by_sev.MEDIUM += 1
        else:                               by_sev.LOW += 1

    return PipelineResponse(
        job_id            = job_id,
        source_batch_id   = batch_id,
        partner_code      = partner_code,
        started_at        = started_at,
        completed_at      = completed_at,
        rows_loaded       = rows_loaded,
        rows_unchanged    = rows_unchanged,
        rows_bad          = rows_bad,
        dq_summary        = dq_summary,
        anomalies_total   = len(anomalies),
        anomalies_by_severity = by_sev,
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

    return new_status, resolved_at, replay_batch
