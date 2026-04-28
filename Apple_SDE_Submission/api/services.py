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
    AnomaliesBySeverity, Anomaly, AnomalyContext, AnomalyDashboardStats,
    AnomalyType, AnomalyVisualization, BadRecordEntry, BadRecordStatus,
    BaselineSnapshot, Confidence, DashboardStats, DashboardTotals,
    DQRulePassRate, DQRuleRunResult, DQSummary, DQViolationsBySeverity,
    DetectAnomaliesRequest, DetectAnomaliesResponse, FunnelStats,
    HarmoniseMatch, HarmoniseResponse, HarmoniseSample, JobStatus,
    LoadDataAcceptedResponse, LoadDataProgress, LoadJobStatus, PipelineResponse,
    ResolveAction, Severity, SignalBreakdown, TimeSeriesPoint,
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

# Keys we never want in the harmonise search text:
#   - __row_num: internal bookkeeping
#   - PRODUCT_NAME_VAL: already passed in via raw_product_name (avoids double weight)
#   - CRAWL_TS, COUNTRY_VAL, PARTNER: metadata, not product attributes
#   - FULL PRICE / MONTHLY_INSTALMENT_AMT / INSTALMENT_MONTH: numeric, already structured
_HARMONISE_PAYLOAD_EXCLUDE = frozenset({
    "__row_num", "PRODUCT_NAME_VAL", "CRAWL_TS",
    "COUNTRY_VAL", "PARTNER",
    "FULL PRICE", "MONTHLY_INSTALMENT_AMT", "INSTALMENT_MONTH",
})


def _build_harmonise_query(raw_product_name: str, raw_payload) -> str:
    """Concatenate raw_product_name + any extra string-typed raw_payload values
    so partner-specific attribute columns (e.g. CONNECTIVITY=WiFi) flow into
    the harmoniser. Numeric fields and known metadata keys are excluded.
    asyncpg returns jsonb as a JSON string, so we parse defensively.
    """
    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError):
            payload = {}
    elif isinstance(raw_payload, dict):
        payload = raw_payload
    else:
        payload = {}

    extras = []
    for key, value in payload.items():
        if key in _HARMONISE_PAYLOAD_EXCLUDE:
            continue
        if not isinstance(value, str):
            continue
        v = value.strip()
        if v:
            extras.append(v)

    if not extras:
        return raw_product_name
    return f"{raw_product_name} {' '.join(extras)}"


async def harmonise_in_stg(conn: asyncpg.Connection, batch_id: uuid.UUID) -> Tuple[int, int, int]:
    """Run the Harmoniser on each row; write product_model_id back to stg.

    Runs on every row in the batch with a raw_product_name. dq_status is
    intentionally NOT filtered — Path A (with prior INGEST DQ) and Path B
    (without prior DQ) both harmonise the whole batch. Path A's INGEST-failing
    rows are still harmonised (negligible cost, prevents missing matches when
    a row fails INGEST for an unrelated reason like NULL crawl_ts).

    Search text is raw_product_name PLUS any other non-numeric string fields
    in raw_payload (excluding the already-structured columns we map elsewhere
    and partner/country/timestamp metadata). This lets the harmoniser pick up
    attribute hints — connectivity (WiFi/Cellular), storage, color — that a
    partner may carry in a separate column instead of embedding in the product
    name. For Partners A and B as currently shaped this is a no-op (Partner A
    has no extra text columns; Partner B already embeds connectivity in
    PRODUCT_NAME_VAL), but it lights up cleanly for any partner that splits
    attributes into their own columns.
    Returns (high, medium, low) confidence counts.
    """
    h = get_harmoniser()

    stg_rows = await conn.fetch(
        """
        SELECT stg_row_id, raw_product_name, raw_payload
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
        query_text = _build_harmonise_query(row["raw_product_name"], row["raw_payload"])
        matches = h.match(query_text, k=1)
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

SEV_RANK = {Severity.HIGH: 3, Severity.MEDIUM: 2, Severity.LOW: 1}

# Severity threshold tiers (relative-deviation signals: STATISTICAL, TEMPORAL,
# CROSS_PARTNER). Hardcoded for the demo; production reads from
# `dim_anomaly_threshold` and snapshots them per fact_anomaly row.
SEV_BAR_LOW    = 0.10
SEV_BAR_MEDIUM = 0.15
SEV_BAR_HIGH   = 0.25

# SKU_VARIANCE uses absolute z-score thresholds (different math)
SKU_Z_LOW    = 1.5
SKU_Z_MEDIUM = 2.5
SKU_Z_HIGH   = 4.0


def _severity_from_pct(pct: float) -> Severity:
    """Map a relative deviation to severity using the standard tier."""
    if pct >= SEV_BAR_HIGH:   return Severity.HIGH
    if pct >= SEV_BAR_MEDIUM: return Severity.MEDIUM
    return Severity.LOW


def _severity_from_zscore(z: float) -> Severity:
    if z >= SKU_Z_HIGH:   return Severity.HIGH
    if z >= SKU_Z_MEDIUM: return Severity.MEDIUM
    return Severity.LOW


async def detect_anomalies_for_batch(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    min_severity: Severity = Severity.LOW,
    product_model_ids: Optional[List[int]] = None,
) -> List[Anomaly]:
    """Run all 4 anomaly signals on the fact rows belonging to this batch
    and persist the union to `fact_anomaly`.

    Signal taxonomy (independent classification per signal — same offer that
    trips two signals = two `fact_anomaly` rows, each with own severity):

    1. STATISTICAL    — vs 30-day rolling baseline from Slowly Changing
                        Dimension Type 2 history. Catches "this is unusual
                        for THIS product right now."
    2. TEMPORAL       — vs the immediate previous price for the same
                        (product, partner, country, payment_type).
                        Catches sudden jumps that the rolling mean smooths.
    3. CROSS_PARTNER  — vs other partners' current price for the same
                        (product, country, payment_type). Catches
                        single-partner pricing errors / data feed bugs.
    4. SKU_VARIANCE   — within-batch variance across observations of the
                        same product on the same crawl date. Catches
                        per-row typos (Space Grey iPad standalone outlier).

    Each detector runs in its own helper and returns an independent list;
    the main function concatenates them, persists to fact_anomaly (idempotent
    via UNIQUE(offer_id, anomaly_type)), and returns the union.
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

    if not offers:
        return []

    anomalies: List[Anomaly] = []
    anomalies.extend(await _detect_statistical(conn, offers, min_severity))
    anomalies.extend(await _detect_temporal(conn, offers, min_severity))
    anomalies.extend(await _detect_cross_partner(conn, offers, min_severity))
    anomalies.extend(await _detect_sku_variance(conn, offers, min_severity))

    if anomalies:
        await _persist_anomalies_to_fact(conn, anomalies, batch_id)
    return anomalies


# ---------- Detector 1: STATISTICAL (vs 30-day rolling baseline) ----------

async def _detect_statistical(
    conn: asyncpg.Connection,
    offers: List[asyncpg.Record],
    min_severity: Severity,
) -> List[Anomaly]:
    """For each offer: compare vs AVG/STDDEV from fact_partner_price_history
    over the last 30 days. Skip if baseline has < 2 samples or deviation < 10%.
    """
    anomalies: List[Anomaly] = []
    for o in offers:
        stat = await conn.fetchrow(
            """
            SELECT AVG(effective_total_usd)    AS mean_usd,
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
        obs      = float(o["effective_total_usd"])
        pct_off  = abs(obs - mean_usd) / mean_usd if mean_usd else 0.0
        if pct_off < SEV_BAR_LOW:
            continue
        severity = _severity_from_pct(pct_off)
        if SEV_RANK[severity] < SEV_RANK[min_severity]:
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
            explanation         = f"{pct_off*100:.1f}% deviation from 30-day mean (${mean_usd:,.2f})",
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
            detected_at   = _now_utc(),
        ))
    return anomalies


# ---------- Detector 2: TEMPORAL (vs immediate previous price) ----------

async def _detect_temporal(
    conn: asyncpg.Connection,
    offers: List[asyncpg.Record],
    min_severity: Severity,
) -> List[Anomaly]:
    """For each offer: compare to the immediately previous price for the same
    (product, partner, country, payment_type) tuple in Slowly Changing
    Dimension Type 2 history. Catches sudden price jumps that the rolling
    mean smooths over.

    'Previous price' = the most recent valid_to_date < current_obs_date row
    (or the open `valid_to_date IS NULL` row when this is a re-observation).
    """
    anomalies: List[Anomaly] = []
    for o in offers:
        prev = await conn.fetchrow(
            """
            SELECT effective_total_usd AS last_usd, valid_from_date
            FROM fact_partner_price_history
            WHERE product_model_id = $1
              AND partner_id       = $2
              AND country_code     = $3
              AND payment_type     = $4::payment_type_enum
              AND valid_from_date  < $5::date
            ORDER BY valid_from_date DESC
            LIMIT 1
            """,
            o["product_model_id"], o["partner_id"], o["country_code"],
            o["payment_type"], o["crawl_ts_utc"],
        )
        if not prev or not prev["last_usd"]:
            continue
        last  = float(prev["last_usd"])
        obs   = float(o["effective_total_usd"])
        if last == 0:
            continue
        pct_off = abs(obs - last) / last
        if pct_off < SEV_BAR_LOW:
            continue
        severity = _severity_from_pct(pct_off)
        if SEV_RANK[severity] < SEV_RANK[min_severity]:
            continue

        anomalies.append(Anomaly(
            anomaly_id          = o["offer_id"],
            offer_id            = o["offer_id"],
            anomaly_type        = AnomalyType.TEMPORAL,
            severity            = severity,
            signal_score        = round(min(pct_off, 1.0), 3),
            product_model_id    = o["product_model_id"],
            product_model_name  = o["model_key"] or "<unknown>",
            partner_code        = o["partner_code"],
            country_code        = o["country_code"],
            observed_price_usd  = round(obs, 2),
            explanation         = (
                f"{pct_off*100:.1f}% jump from last valid price "
                f"(${last:,.2f} on {prev['valid_from_date']})"
            ),
            context = AnomalyContext(
                lifecycle_status    = "STABLE",
                lifecycle_factor    = 1.0,
                suppression_applied = False,
                baseline_snapshot   = BaselineSnapshot(
                    window_days = 0,    # TEMPORAL = single previous point, not a window
                    sample_size = 1,
                    mean        = round(last, 2),
                    stddev      = 0.0,
                ),
            ),
            visualization = None,   # TEMPORAL is point-to-point, no time-series payload
            detected_at   = _now_utc(),
        ))
    return anomalies


# ---------- Detector 3: CROSS_PARTNER (vs peer partners' median) ----------

async def _detect_cross_partner(
    conn: asyncpg.Connection,
    offers: List[asyncpg.Record],
    min_severity: Severity,
) -> List[Anomaly]:
    """For each offer: compare against the median price reported by OTHER
    partners for the same (product, country, payment_type). Reads from
    v_partner_price_current.partner_prices_json which already aggregates
    {partner_code: usd} across the live market snapshot.

    Skip if there's only one partner reporting (no peer to compare to).
    """
    anomalies: List[Anomaly] = []
    for o in offers:
        view_row = await conn.fetchrow(
            """
            SELECT partner_prices_json, partner_count
            FROM v_partner_price_current
            WHERE product_model_id = $1
              AND country_code     = $2
              AND payment_type     = $3::payment_type_enum
            """,
            o["product_model_id"], o["country_code"], o["payment_type"],
        )
        if not view_row or not view_row["partner_prices_json"]:
            continue
        if int(view_row["partner_count"] or 0) < 2:
            continue   # need ≥ 2 partners to have peers

        peer_json = view_row["partner_prices_json"]
        if isinstance(peer_json, str):
            peer_json = json.loads(peer_json)
        # Exclude self
        peer_prices = [
            float(v) for k, v in peer_json.items()
            if k != o["partner_code"] and v is not None
        ]
        if not peer_prices:
            continue

        peer_prices.sort()
        n = len(peer_prices)
        peer_median = (
            peer_prices[n // 2] if n % 2 == 1
            else (peer_prices[n // 2 - 1] + peer_prices[n // 2]) / 2.0
        )

        obs = float(o["effective_total_usd"])
        if peer_median == 0:
            continue
        pct_off = abs(obs - peer_median) / peer_median
        if pct_off < SEV_BAR_LOW:
            continue
        severity = _severity_from_pct(pct_off)
        if SEV_RANK[severity] < SEV_RANK[min_severity]:
            continue

        anomalies.append(Anomaly(
            anomaly_id          = o["offer_id"],
            offer_id            = o["offer_id"],
            anomaly_type        = AnomalyType.CROSS_PARTNER,
            severity            = severity,
            signal_score        = round(min(pct_off, 1.0), 3),
            product_model_id    = o["product_model_id"],
            product_model_name  = o["model_key"] or "<unknown>",
            partner_code        = o["partner_code"],
            country_code        = o["country_code"],
            observed_price_usd  = round(obs, 2),
            explanation         = (
                f"{pct_off*100:.1f}% off the {len(peer_prices)}-peer median "
                f"(${peer_median:,.2f}); peers: " +
                ", ".join(f"{k}=${float(v):,.0f}"
                          for k, v in peer_json.items() if k != o["partner_code"])
            ),
            context = AnomalyContext(
                lifecycle_status    = "STABLE",
                lifecycle_factor    = 1.0,
                suppression_applied = False,
                baseline_snapshot   = BaselineSnapshot(
                    window_days = 0,    # snapshot, not a time window
                    sample_size = len(peer_prices),
                    mean        = round(peer_median, 2),
                    stddev      = 0.0,
                ),
            ),
            visualization = None,
            detected_at   = _now_utc(),
        ))
    return anomalies


# ---------- Detector 4: SKU_VARIANCE (within-batch outlier) ----------

async def _detect_sku_variance(
    conn: asyncpg.Connection,
    offers: List[asyncpg.Record],
    min_severity: Severity,
) -> List[Anomaly]:
    """For each (product_model_id, partner_id, country_code, payment_type,
    crawl_date) group of 3+ observations within the batch: compute mean+std
    and flag rows whose price is > 1.5σ from the group mean as anomalies.

    Catches per-SKU typos like Partner B's Space Grey iPad ($146,340 USD
    while other colors of the same model are ~$1,400). The other colors
    pin the mean tight, the typo blows out as a high z-score.

    Note: this works at the model+date granularity (multi-color SKUs with
    same model_key collapse into one group). True per-SKU resolution would
    require fact_price_offer to carry sku_id (a future schema enhancement).
    """
    if len(offers) < 3:
        return []

    # Group offers by (model, partner, country, payment_type, crawl_date)
    groups: Dict[tuple, List[asyncpg.Record]] = {}
    for o in offers:
        key = (
            o["product_model_id"], o["partner_id"], o["country_code"],
            o["payment_type"], o["crawl_ts_utc"].date(),
        )
        groups.setdefault(key, []).append(o)

    anomalies: List[Anomaly] = []
    for key, group in groups.items():
        if len(group) < 3:
            continue   # need ≥ 3 for variance to be meaningful
        prices = [float(g["effective_total_usd"]) for g in group]
        n_g    = len(prices)
        mean_g = sum(prices) / n_g
        var_g  = sum((p - mean_g) ** 2 for p in prices) / (n_g - 1)
        std_g  = var_g ** 0.5
        if std_g == 0:
            continue   # all prices identical, no outlier possible

        for o in group:
            obs = float(o["effective_total_usd"])
            z   = abs(obs - mean_g) / std_g
            if z < SKU_Z_LOW:
                continue
            severity = _severity_from_zscore(z)
            if SEV_RANK[severity] < SEV_RANK[min_severity]:
                continue

            anomalies.append(Anomaly(
                anomaly_id          = o["offer_id"],
                offer_id            = o["offer_id"],
                anomaly_type        = AnomalyType.SKU_VARIANCE,
                severity            = severity,
                signal_score        = round(min(z / 10.0, 1.0), 3),
                product_model_id    = o["product_model_id"],
                product_model_name  = o["model_key"] or "<unknown>",
                partner_code        = o["partner_code"],
                country_code        = o["country_code"],
                observed_price_usd  = round(obs, 2),
                explanation         = (
                    f"{z:.1f}σ outlier within {n_g}-observation same-model "
                    f"group (mean ${mean_g:,.2f}, σ ${std_g:,.2f})"
                ),
                context = AnomalyContext(
                    lifecycle_status    = "STABLE",
                    lifecycle_factor    = 1.0,
                    suppression_applied = False,
                    baseline_snapshot   = BaselineSnapshot(
                        window_days = 0,   # within-batch snapshot, not time window
                        sample_size = n_g,
                        mean        = round(mean_g, 2),
                        stddev      = round(std_g, 2),
                    ),
                ),
                visualization = None,
                detected_at   = _now_utc(),
            ))
    return anomalies


async def _persist_anomalies_to_fact(
    conn: asyncpg.Connection,
    anomalies: List[Anomaly],
    batch_id: uuid.UUID,
) -> None:
    """UPSERT anomalies into fact_anomaly. Each (offer_id, anomaly_type) pair
    is unique — replaying detection on the same batch UPDATEs the row's
    severity/score/snapshot rather than inserting a duplicate."""
    if not anomalies:
        return

    # Look up crawl_ts_utc + partner_id once per offer (fact_anomaly stores
    # them denormalized for fast filtering without a join)
    offer_ids = list({a.offer_id for a in anomalies})
    meta_rows = await conn.fetch(
        """
        SELECT offer_id, crawl_ts_utc, partner_id
        FROM fact_price_offer
        WHERE offer_id = ANY($1::bigint[])
        """,
        offer_ids,
    )
    offer_meta = {r["offer_id"]: (r["crawl_ts_utc"], r["partner_id"]) for r in meta_rows}

    # Frozen threshold snapshot — what `dim_anomaly_threshold` looked like at
    # detection time. Stored per row so replays remain explainable even if
    # thresholds drift.
    threshold_snap = json.dumps({
        "severity_bar_high":          0.25,
        "severity_bar_medium":        0.15,
        "severity_bar_low":           0.10,
        "signal_weight_statistical":  1.0,
        "min_baseline_samples":       2,
        "baseline_window_days":       30,
    })

    rows: List[tuple] = []
    for a in anomalies:
        meta = offer_meta.get(a.offer_id)
        if not meta:
            continue   # offer was deleted between detect + persist; skip
        crawl_ts, partner_id = meta

        baseline_snap = None
        if a.context.baseline_snapshot is not None:
            bs = a.context.baseline_snapshot
            baseline_snap = json.dumps({
                "mean":        bs.mean,
                "stddev":      bs.stddev,
                "p05":         bs.p05,
                "p50":         bs.p50,
                "p95":         bs.p95,
                "sample_size": bs.sample_size,
                "window_days": bs.window_days,
            })

        rows.append((
            a.offer_id, crawl_ts, batch_id,
            a.product_model_id, partner_id, a.country_code,
            a.anomaly_type.value, float(a.signal_score), a.severity.value,
            a.context.lifecycle_factor, a.context.event_suppression_factor, None,
            float(a.observed_price_usd),
            baseline_snap, threshold_snap,
            a.context.suppression_applied, a.context.suppression_event_id,
        ))

    await conn.executemany(
        """
        INSERT INTO fact_anomaly (
            offer_id, crawl_ts_utc, source_batch_id,
            product_model_id, partner_id, country_code,
            anomaly_type, signal_score, severity,
            lifecycle_factor, event_suppression_factor, category_sensitivity,
            observed_price_usd, baseline_snapshot, threshold_snapshot,
            suppression_applied, suppression_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            $13, $14::jsonb, $15::jsonb, $16, $17
        )
        ON CONFLICT (offer_id, anomaly_type) DO UPDATE SET
            severity                  = EXCLUDED.severity,
            signal_score              = EXCLUDED.signal_score,
            lifecycle_factor          = EXCLUDED.lifecycle_factor,
            event_suppression_factor  = EXCLUDED.event_suppression_factor,
            observed_price_usd        = EXCLUDED.observed_price_usd,
            baseline_snapshot         = EXCLUDED.baseline_snapshot,
            threshold_snapshot        = EXCLUDED.threshold_snapshot,
            suppression_applied       = EXCLUDED.suppression_applied,
            suppression_event_id      = EXCLUDED.suppression_event_id,
            source_batch_id           = EXCLUDED.source_batch_id,
            crawl_ts_utc              = EXCLUDED.crawl_ts_utc,
            detected_at               = NOW(),
            -- preserve workflow fields (status / assignee / resolution_notes)
            -- across re-detections — those are business-set
            status                    = fact_anomaly.status
        """,
        rows,
    )


def _anomaly_from_fact_row(r: asyncpg.Record) -> Anomaly:
    """Re-hydrate an Anomaly Pydantic object from one fact_anomaly row + its
    joined dim_partner / dim_product_model labels. Used by the dashboard read
    path so we don't re-run detection just to render counts."""
    bs = r["baseline_snapshot"]
    if isinstance(bs, str):
        bs = json.loads(bs)
    baseline = None
    if bs:
        baseline = BaselineSnapshot(
            window_days = int(bs.get("window_days", 30)),
            sample_size = int(bs.get("sample_size", 0)),
            mean        = float(bs.get("mean") or 0.0),
            stddev      = bs.get("stddev"),
            p05         = bs.get("p05"),
            p50         = bs.get("p50"),
            p95         = bs.get("p95"),
        )

    obs = float(r["observed_price_usd"]) if r["observed_price_usd"] is not None else 0.0
    mean = baseline.mean if baseline else obs
    pct_off = (abs(obs - mean) / mean) if mean else 0.0
    sig_score = float(r["signal_score"])
    a_type = r["anomaly_type"]

    # Type-aware explanation — each detector has different baseline semantics.
    # Reading from fact_anomaly we don't have the original explanation text
    # (it isn't a stored column), so we re-derive from anomaly_type +
    # baseline_snapshot fields.
    if a_type == "STATISTICAL":
        explanation = (
            f"{pct_off * 100:.1f}% deviation from {baseline.window_days}-day rolling mean "
            f"(${mean:,.2f}, n={baseline.sample_size})"
            if baseline and mean else f"STATISTICAL signal · score {sig_score:.3f}"
        )
    elif a_type == "TEMPORAL":
        explanation = (
            f"{pct_off * 100:.1f}% jump from previous price (${mean:,.2f})"
            if baseline and mean else f"TEMPORAL signal · score {sig_score:.3f}"
        )
    elif a_type == "CROSS_PARTNER":
        explanation = (
            f"{pct_off * 100:.1f}% off the {baseline.sample_size}-peer median "
            f"(${mean:,.2f})"
            if baseline and mean else f"CROSS_PARTNER signal · score {sig_score:.3f}"
        )
    elif a_type == "SKU_VARIANCE":
        # signal_score = z/10 by construction → recover z
        z = sig_score * 10
        explanation = (
            f"{z:.1f}σ outlier within {baseline.sample_size}-observation same-model group "
            f"(group mean ${mean:,.2f}, σ ${baseline.stddev or 0:,.2f})"
            if baseline else f"SKU_VARIANCE signal · score {sig_score:.3f}"
        )
    else:
        explanation = f"{a_type} signal · score {sig_score:.3f}"

    return Anomaly(
        anomaly_id          = int(r["anomaly_id"]),
        offer_id            = int(r["offer_id"]),
        anomaly_type        = AnomalyType(r["anomaly_type"]),
        severity            = Severity(r["severity"]),
        signal_score        = float(r["signal_score"]),
        product_model_id    = int(r["product_model_id"]),
        product_model_name  = r["model_key"] or "<unknown>",
        partner_code        = r["partner_code"],
        country_code        = r["country_code"],
        observed_price_usd  = round(obs, 2),
        explanation         = explanation,
        context = AnomalyContext(
            lifecycle_status         = "STABLE",
            lifecycle_factor         = float(r["lifecycle_factor"] or 1.0),
            event_suppression_factor = float(r["event_suppression_factor"] or 1.0),
            suppression_applied      = bool(r["suppression_applied"]),
            suppression_event_id     = r["suppression_event_id"],
            baseline_snapshot        = baseline,
        ),
        visualization = None,  # built on-demand for the chart sample only
        detected_at   = r["detected_at"],
    )


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

        # Per-stage failed-record counts (for the frontend's per-step view)
        stage_rows = await conn.fetch(
            """
            SELECT c.target_stage AS stage, SUM(o.failed_records) AS fail
            FROM dq_output o
            JOIN dq_rule_catalog c ON c.rule_id = o.rule_id
            WHERE o.source_batch_id = $1
            GROUP BY c.target_stage
            """,
            batch_id,
        )
        dq_by_stage: Dict[str, int] = {"INGEST": 0, "PRE_FACT": 0, "SEMANTIC": 0}
        for r in stage_rows:
            dq_by_stage[r["stage"]] = int(r["fail"] or 0)

        # Counters for the frontend's "table changes per step" view
        rows_stg = (await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM stg_price_offer WHERE source_batch_id = $1",
            batch_id,
        ))["n"] or 0
        rows_history = (await conn.fetchrow(
            """
            SELECT COUNT(*) AS n FROM fact_partner_price_history
            WHERE (product_model_id, partner_id, country_code, payment_type) IN (
                SELECT DISTINCT product_model_id, partner_id, country_code, payment_type
                FROM fact_price_offer WHERE source_batch_id = $1
            )
            """,
            batch_id,
        ))["n"] or 0

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
        rows_stg          = rows_stg,
        rows_loaded       = rows_loaded,
        rows_unchanged    = rows_unchanged,
        rows_bad          = rows_bad,
        rows_history      = rows_history,
        dq_summary        = dq_summary,
        dq_by_stage       = dq_by_stage,
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


# ===========================================================================
# /dashboard-stats — DB-wide aggregate snapshot for the live UI
# ===========================================================================

async def get_dashboard_stats(sample_bad_limit: int = 12) -> DashboardStats:
    """DB-wide aggregate stats — totals, harmonise distribution, DQ pass rates,
    funnel, dedicated lists for PRE_FACT-blocked + LOW-confidence harmonise,
    a stratified harmonise-sample set with full signal breakdown, and an
    anomaly visualization. Powers submission/pipeline_runner.html.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        # ----- Totals -----
        rows_stg     = (await conn.fetchrow("SELECT COUNT(*) AS n FROM stg_price_offer"))["n"]
        rows_fact    = (await conn.fetchrow("SELECT COUNT(*) AS n FROM fact_price_offer"))["n"]
        rows_history = (await conn.fetchrow("SELECT COUNT(*) AS n FROM fact_partner_price_history"))["n"]
        rows_bad     = (await conn.fetchrow("SELECT COUNT(*) AS n FROM dq_bad_records"))["n"]
        batches      = (await conn.fetchrow("SELECT COUNT(*) AS n FROM dws_partner_dq_per_batch"))["n"]
        rules_active = (await conn.fetchrow("SELECT COUNT(*) AS n FROM dq_rule_catalog WHERE is_active"))["n"]

        # Distinct rows blocked at PRE_FACT — a row failing multiple PRE_FACT rules counts once
        blocked = (await conn.fetchrow(
            """
            SELECT COUNT(*) AS n FROM (
              SELECT DISTINCT b.source_batch_id, (b.raw_payload->>'__row_num')::bigint AS row_num
              FROM dq_bad_records b
              JOIN dq_rule_catalog c ON c.rule_id = b.rule_id
              WHERE c.target_stage = 'PRE_FACT'
            ) x
            """
        ))["n"]

        # ----- Harmonise confidence distribution -----
        harm_rows = await conn.fetch(
            """
            SELECT harmonise_confidence::text AS conf, COUNT(*) AS n
            FROM fact_price_offer
            WHERE harmonise_confidence IS NOT NULL
            GROUP BY harmonise_confidence
            """
        )
        harmonise = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "MANUAL": 0}
        for r in harm_rows:
            if r["conf"] in harmonise:
                harmonise[r["conf"]] = int(r["n"])
        harm_total = sum(harmonise.values())
        harm_high_pct = (harmonise["HIGH"] / harm_total) if harm_total > 0 else 0.0

        # ----- DQ violations by severity (DB-wide) -----
        sev_rows = await conn.fetch(
            "SELECT severity::text AS sev, COUNT(*) AS n FROM dq_bad_records GROUP BY severity"
        )
        by_sev = DQViolationsBySeverity()
        for r in sev_rows:
            s = (r["sev"] or "MEDIUM").upper()
            n = int(r["n"])
            if   s == "HIGH":   by_sev.HIGH   += n
            elif s == "LOW":    by_sev.LOW    += n
            else:               by_sev.MEDIUM += n

        # ----- DQ violations by stage -----
        stage_rows = await conn.fetch(
            """
            SELECT c.target_stage AS stage, COUNT(*) AS n
            FROM dq_bad_records b
            JOIN dq_rule_catalog c ON c.rule_id = b.rule_id
            GROUP BY c.target_stage
            """
        )
        by_stage = {"INGEST": 0, "PRE_FACT": 0, "SEMANTIC": 0}
        for r in stage_rows:
            by_stage[r["stage"]] = int(r["n"])

        # ----- Per-rule pass rates aggregated across all batches -----
        pr_rows = await conn.fetch(
            """
            SELECT
              c.rule_id, c.rule_name, c.rule_category, c.severity::text AS severity,
              c.target_stage, c.description,
              COALESCE(SUM(o.total_records), 0)  AS total,
              COALESCE(SUM(o.failed_records), 0) AS failed
            FROM dq_rule_catalog c
            LEFT JOIN dq_output o ON o.rule_id = c.rule_id
            WHERE c.is_active
            GROUP BY c.rule_id, c.rule_name, c.rule_category, c.severity, c.target_stage, c.description
            ORDER BY
              CASE c.target_stage WHEN 'INGEST' THEN 1 WHEN 'PRE_FACT' THEN 2 ELSE 3 END,
              c.rule_id
            """
        )
        pass_rates = []
        for r in pr_rows:
            total  = int(r["total"]  or 0)
            failed = int(r["failed"] or 0)
            pr     = (1.0 - failed / total) if total > 0 else 1.0
            pass_rates.append(DQRulePassRate(
                rule_id        = r["rule_id"],
                rule_name      = r["rule_name"],
                rule_category  = r["rule_category"] or "general",
                severity       = Severity(r["severity"] or "MEDIUM"),
                target_stage   = r["target_stage"],
                description    = r["description"],
                total_records  = total,
                failed_records = failed,
                pass_rate      = round(pr, 4),
            ))

        # ----- Funnel -----
        ingest_passed   = (await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM stg_price_offer WHERE dq_status IN ('INGEST_PASSED','PRE_FACT_PASSED')"
        ))["n"]
        after_harmonise = (await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM stg_price_offer WHERE product_model_id IS NOT NULL"
        ))["n"]
        prefact_passed  = (await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM stg_price_offer WHERE dq_status = 'PRE_FACT_PASSED'"
        ))["n"]
        funnel = FunnelStats(
            stg             = rows_stg,
            ingest_passed   = ingest_passed,
            after_harmonise = after_harmonise,
            prefact_passed  = prefact_passed,
            fact            = rows_fact,
            history         = rows_history,
        )

        def _row_to_bad_entry(r: asyncpg.Record, target_stage: Optional[str] = None) -> BadRecordEntry:
            return BadRecordEntry(
                bad_record_id   = r["bad_record_id"],
                source_batch_id = r["source_batch_id"],
                rule_id         = r["rule_id"],
                target_stage    = target_stage if target_stage is not None else r.get("target_stage"),
                failed_field    = r["failed_field"],
                error_message   = r["error_message"],
                severity        = Severity(r["severity"] or "MEDIUM"),
                status          = BadRecordStatus(r["status"]),
                assignee        = r["assignee"],
                raw_payload     = json.loads(r["raw_payload"]) if isinstance(r["raw_payload"], str) else r["raw_payload"],
                detected_at     = r["detected_at"],
                resolved_at     = r["resolved_at"],
            )

        # ----- Recent bad records (any rule) — keeps a small sample -----
        bad_rows = await conn.fetch(
            f"""
            SELECT b.bad_record_id, b.source_batch_id, b.rule_id, c.target_stage,
                   b.failed_field, b.error_message, b.severity, b.status::text AS status,
                   b.assignee, b.raw_payload, b.detected_at, b.resolved_at
            FROM dq_bad_records b
            JOIN dq_rule_catalog c ON c.rule_id = b.rule_id
            ORDER BY b.detected_at DESC
            LIMIT {int(sample_bad_limit)}
            """
        )
        sample_bad = [_row_to_bad_entry(r) for r in bad_rows]

        # ----- Full distinct list of PRE_FACT-blocked rows -----
        # A row that fails multiple PRE_FACT rules appears once (DISTINCT ON
        # the (batch_id, row_num) tuple, keeping the first detected occurrence).
        prefact_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (b.source_batch_id, (b.raw_payload->>'__row_num')::bigint)
                   b.bad_record_id, b.source_batch_id, b.rule_id, c.target_stage,
                   b.failed_field, b.error_message, b.severity, b.status::text AS status,
                   b.assignee, b.raw_payload, b.detected_at, b.resolved_at
            FROM dq_bad_records b
            JOIN dq_rule_catalog c ON c.rule_id = b.rule_id
            WHERE c.target_stage = 'PRE_FACT'
            ORDER BY b.source_batch_id, (b.raw_payload->>'__row_num')::bigint, b.detected_at
            """
        )
        prefact_blocked = [_row_to_bad_entry(r) for r in prefact_rows]

        # ----- Full LOW-confidence harmonise list (DQ_HARM_001) -----
        low_conf_rows = await conn.fetch(
            """
            SELECT b.bad_record_id, b.source_batch_id, b.rule_id, c.target_stage,
                   b.failed_field, b.error_message, b.severity, b.status::text AS status,
                   b.assignee, b.raw_payload, b.detected_at, b.resolved_at
            FROM dq_bad_records b
            JOIN dq_rule_catalog c ON c.rule_id = b.rule_id
            WHERE b.rule_id = 'DQ_HARM_001'
            ORDER BY b.detected_at DESC
            """
        )
        low_conf_harmonise = [_row_to_bad_entry(r) for r in low_conf_rows]

        # ----- Harmonise samples (stratified by confidence) with full signal breakdown -----
        harmonise_samples = await _build_harmonise_samples(conn)

        # ----- Anomaly stats — run detection on the most recent batch -----
        anomaly_stats = await _build_anomaly_dashboard(conn)

        return DashboardStats(
            snapshot_at               = _now_utc(),
            totals = DashboardTotals(
                rows_stg                = rows_stg,
                rows_fact               = rows_fact,
                rows_history            = rows_history,
                rows_bad                = rows_bad,
                rows_blocked_by_prefact = blocked,
                harmonise_high_pct      = round(harm_high_pct, 4),
                total_violations        = rows_bad,
                batches_loaded          = batches,
                rules_in_catalog        = rules_active,
            ),
            harmonise_confidence              = harmonise,
            dq_violations_by_severity         = by_sev,
            dq_violations_by_stage            = by_stage,
            dq_pass_rates                     = pass_rates,
            funnel                            = funnel,
            sample_bad_records                = sample_bad,
            prefact_blocked_records           = prefact_blocked,
            low_confidence_harmonise_records  = low_conf_harmonise,
            harmonise_samples                 = harmonise_samples,
            anomaly_stats                     = anomaly_stats,
        )


async def _build_harmonise_samples(conn: asyncpg.Connection) -> List[HarmoniseSample]:
    """Pick representative raw_product_name values from fact (1 HIGH-top, 1
    HIGH-verbose, 1 MEDIUM, 1 LOW) and re-score each via the in-process
    harmoniser to surface the per-signal breakdown the showcase shows."""
    samples_meta: List[Tuple[str, str]] = []  # (raw_product_name, note)

    # Top-scoring HIGH (typically a clean abbreviation that hit attr_match override)
    top_high = await conn.fetchrow(
        """
        SELECT raw_product_name
        FROM fact_price_offer
        WHERE harmonise_confidence = 'HIGH'
        ORDER BY harmonise_score DESC, LENGTH(raw_product_name) ASC
        LIMIT 1
        """
    )
    if top_high:
        samples_meta.append((top_high["raw_product_name"], "Top score in DB · structural override likely fired"))

    # Long verbose HIGH (illustrates 'verbose-but-correct' robustness)
    verbose_high = await conn.fetchrow(
        """
        SELECT raw_product_name
        FROM fact_price_offer
        WHERE harmonise_confidence = 'HIGH'
          AND LENGTH(raw_product_name) >= 60
        ORDER BY LENGTH(raw_product_name) DESC
        LIMIT 1
        """
    )
    if verbose_high and (not samples_meta or verbose_high["raw_product_name"] != samples_meta[0][0]):
        samples_meta.append((verbose_high["raw_product_name"], "Verbose partner string · attribute match dominates over token noise"))

    # MEDIUM
    medium = await conn.fetchrow(
        """
        SELECT raw_product_name
        FROM fact_price_offer
        WHERE harmonise_confidence = 'MEDIUM'
        ORDER BY harmonise_score DESC
        LIMIT 1
        """
    )
    if medium:
        samples_meta.append((medium["raw_product_name"], "Medium confidence · partial signal overlap, manual review optional"))

    # LOW (drives the SEMANTIC flagging path — exactly what's in dq_bad_records)
    low = await conn.fetchrow(
        """
        SELECT raw_product_name
        FROM fact_price_offer
        WHERE harmonise_confidence = 'LOW'
        ORDER BY harmonise_score ASC
        LIMIT 1
        """
    )
    if low:
        samples_meta.append((low["raw_product_name"], "Low confidence · flagged via DQ_HARM_001 (SEMANTIC stage)"))

    if not samples_meta:
        return []

    # Re-score via the in-process harmoniser for the full SignalBreakdown
    h = get_harmoniser()
    samples: List[HarmoniseSample] = []
    for raw, note in samples_meta:
        matches = h.match(raw, k=1)
        if not matches:
            continue
        m = matches[0]
        samples.append(HarmoniseSample(
            raw_product_name = raw,
            canonical_name   = m.canonical_name,
            model_key        = m.model_key,
            sku_ids          = m.sku_ids,
            score            = round(m.score, 3),
            confidence       = Confidence(m.confidence),
            signal_breakdown = SignalBreakdown(
                attr_match      = m.breakdown.attr_match,
                token_jaccard   = m.breakdown.token_jaccard,
                char_fuzz       = m.breakdown.char_fuzz,
                attr_matched    = m.breakdown.attr_matches_on,
                attr_mismatched = m.breakdown.attr_mismatches,
            ),
            note             = note,
        ))
    return samples


async def _build_anomaly_dashboard(conn: asyncpg.Connection) -> AnomalyDashboardStats:
    """Read persisted anomalies from `fact_anomaly` (no re-detection) and
    aggregate stats for the dashboard.

       1. SELECT recent N rows from fact_anomaly (last 30 days), sorted by
          severity DESC, signal_score DESC — fact_anomaly is the source of
          truth. detect_anomalies_for_batch wrote them at ingest time.
       2. Aggregate by_severity + by_type, build top-N table.
       3. Pick a chart anomaly: prefer the absolute top if it's STATISTICAL
          (only signal whose payload is a 30-day time-series); otherwise the
          highest-ranked STATISTICAL anomaly in the recent window so the Live
          Sample card never goes empty. If no STATISTICAL anomaly exists,
          render a baseline-only illustration via
          `_build_baseline_only_visualization`. The caption notes whichever
          fallback was used.
       4. If 0 anomalies → same baseline-only fallback (helper shared).

    DQ rule violations are NOT merged into this anomaly stream — they have
    their own review path in dq_bad_records and the Task C-2 closure loop.
    """
    RECENT_ANOMALY_TABLE_LIMIT = 20
    LOOKBACK_DAYS = 30

    fact_rows = await conn.fetch(
        f"""
        SELECT
          a.anomaly_id, a.offer_id, a.anomaly_type, a.signal_score, a.severity,
          a.observed_price_usd, a.lifecycle_factor, a.event_suppression_factor,
          a.suppression_applied, a.suppression_event_id,
          a.baseline_snapshot, a.threshold_snapshot, a.detected_at,
          a.country_code, a.product_model_id,
          p.partner_code,
          m.model_key
        FROM fact_anomaly a
        JOIN dim_partner       p ON p.partner_id       = a.partner_id
        JOIN dim_product_model m ON m.product_model_id = a.product_model_id
        WHERE a.detected_at >= NOW() - INTERVAL '{LOOKBACK_DAYS} days'
        ORDER BY
          CASE a.severity
            WHEN 'HIGH' THEN 3
            WHEN 'MEDIUM' THEN 2
            ELSE 1
          END DESC,
          a.signal_score DESC,
          a.detected_at DESC
        LIMIT {RECENT_ANOMALY_TABLE_LIMIT * 5}
        """
    )

    anomalies: List[Anomaly] = [_anomaly_from_fact_row(r) for r in fact_rows]

    by_sev = AnomaliesBySeverity()
    by_type: Dict[str, int] = {}
    for a in anomalies:
        if a.severity == Severity.HIGH:     by_sev.HIGH   += 1
        elif a.severity == Severity.MEDIUM: by_sev.MEDIUM += 1
        else:                                by_sev.LOW    += 1
        by_type[a.anomaly_type.value] = by_type.get(a.anomaly_type.value, 0) + 1

    recent_top = anomalies[:RECENT_ANOMALY_TABLE_LIMIT]

    if anomalies:
        top = recent_top[0]
        # The time-series chart only renders cleanly for STATISTICAL — that
        # signal's payload (30-day series + ±1σ band) is what
        # `_build_anomaly_visualization` produces. TEMPORAL/CROSS_PARTNER/
        # SKU_VARIANCE are point-in-time comparisons and don't map to a 30-day
        # line chart. To avoid an empty Live Sample, we use the top STATISTICAL
        # anomaly's chart when the absolute top isn't STATISTICAL, and fall
        # back to a baseline-only chart from the richest history group when no
        # STATISTICAL anomaly exists at all.
        chart_anomaly = top if top.anomaly_type == AnomalyType.STATISTICAL else next(
            (a for a in anomalies if a.anomaly_type == AnomalyType.STATISTICAL), None
        )
        top_payload = None
        if chart_anomaly is not None:
            top_payload = await _build_anomaly_visualization(
                conn,
                product_model_id = chart_anomaly.product_model_id,
                partner_id       = (await conn.fetchval(
                    "SELECT partner_id FROM dim_partner WHERE partner_code=$1", chart_anomaly.partner_code,
                )),
                country_code     = chart_anomaly.country_code,
                payment_type     = await conn.fetchval(
                    "SELECT payment_type::text FROM fact_price_offer WHERE offer_id=$1", chart_anomaly.offer_id,
                ) or "FULL",
                anomaly_date     = chart_anomaly.detected_at.date() if chart_anomaly.detected_at else _now_utc().date(),
                anomaly_price    = float(chart_anomaly.observed_price_usd),
                mean_usd         = (chart_anomaly.context.baseline_snapshot.mean
                                    if chart_anomaly.context.baseline_snapshot else float(chart_anomaly.observed_price_usd)),
                std_usd          = (chart_anomaly.context.baseline_snapshot.stddev or 0.0
                                    if chart_anomaly.context.baseline_snapshot else 0.0),
            )
        else:
            top_payload = await _build_baseline_only_visualization(conn)

        caption_top = (
            f"Top: {top.anomaly_type.value} · {top.product_model_name} · "
            f"{top.partner_code} · {top.country_code} · observed "
            f"${top.observed_price_usd:,.2f} ({top.severity.value}) — {top.explanation}"
        )
        if chart_anomaly is None:
            chart_note = " · chart shows baseline-only illustration (top signal is point-in-time, no STATISTICAL anomaly to plot)"
        elif chart_anomaly is not top:
            chart_note = (
                f" · chart shows next-best STATISTICAL anomaly "
                f"({chart_anomaly.product_model_name} · {chart_anomaly.partner_code})"
            )
        else:
            chart_note = ""
        caption = (
            f"{caption_top}{chart_note} · "
            f"read {len(anomalies)} anomaly row{'s' if len(anomalies) != 1 else ''} from "
            f"fact_anomaly (last {LOOKBACK_DAYS} days)"
        )
        return AnomalyDashboardStats(
            total_detected       = len(anomalies),
            by_severity          = by_sev,
            by_type              = by_type,
            sample_visualization = top_payload,
            sample_caption       = caption,
            sample_severity      = top.severity,
            is_real_anomaly      = True,
            recent_anomalies     = recent_top,
        )

    # Fallback — no anomalies at all. Use the same baseline-only illustration
    # as the non-STATISTICAL chart-fallback path so the dashboard never shows
    # an empty Live Sample card.
    sample_viz, baseline_caption = await _build_baseline_only_visualization(
        conn, return_caption=True,
    )
    if sample_viz is None:
        return AnomalyDashboardStats(
            total_detected       = 0,
            by_severity          = AnomaliesBySeverity(),
            by_type              = {},
            sample_visualization = None,
            sample_caption       = "No anomalies detected · fact_price_offer too sparse for an illustrative baseline",
            sample_severity      = None,
            is_real_anomaly      = False,
            recent_anomalies     = [],
        )
    return AnomalyDashboardStats(
        total_detected       = 0,
        by_severity          = AnomaliesBySeverity(),
        by_type              = {},
        sample_visualization = sample_viz,
        sample_caption       = f"No anomalies in the latest batch — {baseline_caption}",
        sample_severity      = None,
        is_real_anomaly      = False,
        recent_anomalies     = [],
    )


async def _build_baseline_only_visualization(
    conn: asyncpg.Connection,
    *,
    return_caption: bool = False,
):
    """Pick the richest (model, partner, country, payment_type) group in
    fact_price_offer and return its time-series + ±1σ band.

    Used as a fallback when the dashboard's top anomaly isn't STATISTICAL (so
    its native chart wouldn't be a 30-day series) or when there are no
    anomalies at all. Excludes DQ_PRICE_001-flagged offers so 100x-typo
    outliers don't dominate the variance ranking.

    Returns AnomalyVisualization or None if fact is too sparse. When
    return_caption=True returns (viz, caption_str) — caption is empty for the
    None case.
    """
    top_hist = await conn.fetchrow(
        """
        WITH clean AS (
          SELECT f.product_model_id, f.partner_id, f.country_code,
                 f.payment_type, f.effective_total_usd
          FROM fact_price_offer f
          WHERE f.offer_id NOT IN (
            SELECT (b.raw_payload->>'offer_id')::bigint
            FROM dq_bad_records b
            WHERE b.rule_id = 'DQ_PRICE_001'
              AND b.raw_payload ? 'offer_id'
          )
        )
        SELECT product_model_id, partner_id, country_code,
               payment_type::text AS payment_type,
               COUNT(*) AS n,
               COALESCE(STDDEV(effective_total_usd), 0) AS sigma
        FROM clean
        GROUP BY product_model_id, partner_id, country_code, payment_type
        HAVING COUNT(*) >= 3
        ORDER BY (COALESCE(STDDEV(effective_total_usd), 0) > 0) DESC,
                 COALESCE(STDDEV(effective_total_usd), 0) DESC,
                 COUNT(*) DESC
        LIMIT 1
        """
    )
    if not top_hist or top_hist["n"] < 2:
        return (None, "") if return_caption else None

    series_rows = await conn.fetch(
        """
        SELECT crawl_ts_utc::date AS d,
               AVG(effective_total_usd)::numeric(12,2) AS p
        FROM fact_price_offer
        WHERE product_model_id = $1
          AND partner_id       = $2
          AND country_code     = $3
          AND payment_type     = $4::payment_type_enum
          AND offer_id NOT IN (
            SELECT (b.raw_payload->>'offer_id')::bigint
            FROM dq_bad_records b
            WHERE b.rule_id = 'DQ_PRICE_001'
              AND b.raw_payload ? 'offer_id'
          )
        GROUP BY crawl_ts_utc::date
        ORDER BY crawl_ts_utc::date
        """,
        top_hist["product_model_id"], top_hist["partner_id"],
        top_hist["country_code"], top_hist["payment_type"],
    )
    prices = [float(r["p"]) for r in series_rows]
    mean = sum(prices) / len(prices) if prices else 0.0
    if len(prices) > 1:
        var = sum((p - mean) ** 2 for p in prices) / (len(prices) - 1)
        std = var ** 0.5
    else:
        std = 0.0

    sample_viz = AnomalyVisualization(
        type="time_series_with_band",
        series=[TimeSeriesPoint(date=r["d"], price_usd=float(r["p"])) for r in series_rows],
        baseline_band={
            "mean":  round(mean, 2),
            "lower": round(max(0.0, mean - std), 2),
            "upper": round(mean + std, 2),
        },
        cross_partner_comparison=None,
    )
    if not return_caption:
        return sample_viz

    model_label = await conn.fetchval(
        "SELECT model_key FROM dim_product_model WHERE product_model_id = $1",
        top_hist["product_model_id"],
    )
    partner_label = await conn.fetchval(
        "SELECT partner_code FROM dim_partner WHERE partner_id = $1",
        top_hist["partner_id"],
    )
    caption = (
        f"illustrative baseline from {model_label or 'top-history product'} · "
        f"{partner_label or '?'} · {top_hist['country_code']} · "
        f"{top_hist['payment_type']} ({top_hist['n']} history points)"
    )
    return sample_viz, caption
