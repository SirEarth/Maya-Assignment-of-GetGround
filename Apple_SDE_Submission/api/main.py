"""
FastAPI application — entry point.

Architecture: one independent API service. Inside it:
  • 4 Task-B sub-modules — POST /load-data, POST /compute-dq,
    POST /detect-anomalies, GET /harmonise-product. Each is independently
    callable and does ONE thing (a coherent group of pipeline steps).
  • 1 orchestrator — POST /pipeline. Runs the canonical 9-step pipeline
    end-to-end in interleaved order with the PRE_FACT hard gate enabled.

Both paths share the same 9 internal step helpers in api/services.py — zero
code duplication. Path A (/pipeline) hard-gates bad rows out of fact_price_offer.
Path B (sub-modules called individually) covers all 9 steps but the gate
degrades to post-hoc flagging in dq_bad_records.

Run from the unzipped project folder (containing this README):
    uvicorn api.main:app --reload --port 8000

Open:
    http://localhost:8000/docs   — interactive Swagger UI
    http://localhost:8000/redoc  — clean ReDoc UI
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

from fastapi import (
    FastAPI, File, Form, HTTPException, Path, Query, UploadFile, status,
)

from . import services
from .db import close_pool, init_pool
from .models import (
    BadRecordList, BadRecordStatus, ComputeDQRequest, ComputeDQResponse,
    Confidence, DetectAnomaliesRequest, DetectAnomaliesResponse,
    ErrorResponse, HarmoniseResponse, LoadDataAcceptedResponse, LoadJobStatus,
    PipelineResponse, ResolveBadRecordRequest, ResolveBadRecordResponse,
    Severity,
)

# ---------------------------------------------------------------------------
# Lifespan — manages PostgreSQL connection pool
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the asyncpg pool on startup; close it on shutdown."""
    await init_pool()
    yield
    await close_pool()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Pricing Pipeline API",
    description=(
        "Multi-partner pricing ingestion + harmonisation + Data Quality "
        "+ anomaly detection.\n\n"
        "**Path A — POST /pipeline**: one-click 9-step orchestrator.\n"
        "**Path B — sub-modules**: /load-data, /compute-dq, /detect-anomalies, "
        "/harmonise-product. Independently callable; combine in sequence to "
        "cover the same 9 steps with degraded gating semantics."
    ),
    version="0.2.0",
    contact={"name": "huizhongwu"},
    lifespan=lifespan,
)


@app.get("/health", tags=["meta"], summary="Liveness probe")
def health():
    """Standard liveness probe for container-orchestration platforms."""
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


# ===========================================================================
# Path A — POST /pipeline (orchestrator)
# ===========================================================================

@app.post(
    "/pipeline",
    tags=["pipeline"],
    response_model=PipelineResponse,
    responses={400: {"model": ErrorResponse}},
    summary="One-click orchestrator — runs all 9 steps end-to-end",
    description=(
        "Runs the canonical 9-step pipeline in interleaved order:\n\n"
        "  1. Parse CSV → `stg_price_offer`\n"
        "  2. INGEST-stage Data Quality\n"
        "  3. Harmonise raw product names\n"
        "  4. PRE_FACT-stage Data Quality (HIGH-severity gate)\n"
        "  5. Insert into `fact_price_offer` + payment child "
        "(**only PRE_FACT-passing rows**)\n"
        "  6. SEMANTIC-stage Data Quality (soft signals; flag-and-keep)\n"
        "  7. Update Slowly Changing Dimension Type 2 history\n"
        "  8. Detect pricing anomalies on the new batch\n"
        "  9. Write per-batch summary to `dws_partner_dq_per_batch`\n\n"
        "Single transaction. PRE_FACT bad rows are hard-blocked from `fact_price_offer`. "
        "Equivalent to calling /load-data + /compute-dq + /detect-anomalies in "
        "sequence except for stronger gating semantics (the sub-module path "
        "lets bad rows enter fact and flags them post-hoc)."
    ),
)
async def pipeline(
    file:         UploadFile = File(..., description="CSV file"),
    partner_code: str        = Form(..., description="Partner identifier registered in dim_partner"),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Only .csv files are accepted")
    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Empty file")
    return await services.run_pipeline(contents, partner_code)


# ===========================================================================
# Path B — sub-module endpoints (Task B endpoints)
# ===========================================================================

# ---------- POST /load-data — Task B endpoint #1 ----------

@app.post(
    "/load-data",
    tags=["load-data"],
    status_code=status.HTTP_202_ACCEPTED,
    response_model=LoadDataAcceptedResponse,
    responses={400: {"model": ErrorResponse}},
    summary="Load CSV into the table (parse + harmonise + write fact + Slowly Changing Dimension Type 2)",
    description=(
        "Task B sub-module — covers pipeline steps **1, 3, 5, 7, 9**:\n\n"
        "  1. Parse CSV into `stg_price_offer`\n"
        "  3. Harmonise raw product names against the canonical registry\n"
        "  5. Insert change events into `fact_price_offer` + payment child "
        "(**no PRE_FACT gate** — all rows with parseable fields enter fact; "
        "Data Quality flagging happens post-hoc via /compute-dq)\n"
        "  7. Update Slowly Changing Dimension Type 2 history\n"
        "  9. Write batch summary\n\n"
        "Steps 2/4/6 (Data Quality) and step 8 (anomaly detection) are NOT run here — "
        "call /compute-dq and /detect-anomalies separately, or use POST /pipeline "
        "for end-to-end with hard gating."
    ),
)
async def load_data(
    file:         UploadFile = File(..., description="CSV file"),
    partner_code: str        = Form(..., description="Partner identifier registered in dim_partner"),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Only .csv files are accepted")
    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Empty file")
    return await services.submit_load_job(contents, partner_code)


@app.get(
    "/load-data/{job_id}",
    tags=["load-data"],
    response_model=LoadJobStatus,
    responses={404: {"model": ErrorResponse}},
    summary="Poll the progress / status of a load job",
)
def get_load_job(
    job_id: UUID = Path(..., description="The job_id returned by POST /load-data"),
):
    return services.get_job_status(job_id)


# ---------- POST /compute-dq — Task B endpoint #2 ----------

@app.post(
    "/compute-dq",
    tags=["compute-dq"],
    response_model=ComputeDQResponse,
    summary="Validate Data Quality — runs all 13 rules and writes to dq_output + dq_bad_records",
    description=(
        "Task B sub-module — covers pipeline steps **2, 4, 6, 9**:\n\n"
        "  2. INGEST-stage Data Quality (parse / format / required-field rules)\n"
        "  4. PRE_FACT-stage Data Quality (country↔currency, partner↔country, harmonise match)\n"
        "  6. SEMANTIC-stage Data Quality (soft signals — low-confidence harmonise, "
        "category sanity bounds)\n"
        "  9. Refresh batch summary\n\n"
        "All 13 rules execute against the batch. Per-rule pass rates are "
        "stored in **`dq_output`**; per-row violations (with the original CSV "
        "payload preserved as JSONB) are stored in **`dq_bad_records`**.\n\n"
        "Designed to be called AFTER /load-data has populated fact_price_offer. "
        "PRE_FACT-failing rows are flagged post-hoc — they remain in fact and "
        "must be filtered via `LEFT JOIN dq_bad_records` if downstream queries "
        "need a clean view. For end-to-end execution with hard gating, use "
        "POST /pipeline instead."
    ),
)
async def compute_dq(req: ComputeDQRequest):
    started = datetime.now(timezone.utc)
    summary, rule_runs = await services.compute_dq_service(req.source_batch_id)
    completed = datetime.now(timezone.utc)
    return ComputeDQResponse(
        source_batch_id = req.source_batch_id,
        started_at      = started,
        completed_at    = completed,
        summary         = summary,
        rule_runs       = rule_runs,
    )


# ---------- POST /detect-anomalies — Task B endpoint #3 ----------

@app.post(
    "/detect-anomalies",
    tags=["detect-anomalies"],
    response_model=DetectAnomaliesResponse,
    summary="Detect pricing anomalies + return visualization payload",
    description=(
        "Task B sub-module — covers pipeline step **8** (and refreshes step 9 summary).\n\n"
        "For each offer in scope, compares its USD price against the 30-day "
        "rolling mean from `fact_partner_price_history` (the Slowly Changing "
        "Dimension Type 2 table). Returns the anomaly list with severity "
        "classification (HIGH ≥25%, MEDIUM ≥15%, LOW ≥10%) and a structured "
        "visualization payload (time series + baseline band + cross-partner "
        "comparison) that the frontend can render directly via Chart.js / Recharts."
    ),
)
async def detect_anomalies(req: DetectAnomaliesRequest):
    if not req.source_batch_id and not req.date_range:
        raise HTTPException(400, "Must specify either source_batch_id or date_range")
    return await services.detect_anomalies_service(req)


# ---------- GET /harmonise-product — Task B endpoint #4 ----------

@app.get(
    "/harmonise-product",
    tags=["harmonise-product"],
    response_model=HarmoniseResponse,
    summary="Harmonise a raw product name to canonical model(s) — Top-K + score",
    description=(
        "Task B sub-module — exposes the harmoniser as an ad-hoc lookup. "
        "Returns up to **k** ranked candidate matches for a partner-supplied "
        "product name. Each candidate has a 0-1 score, a confidence bucket "
        "(HIGH / MEDIUM / LOW / MANUAL) and a per-signal breakdown for "
        "explainability.\n\n"
        "Internally, pipeline step 3 (`harmonise_in_stg`) calls this same "
        "harmoniser for every staged row during /load-data and /pipeline."
    ),
)
def harmonise_product(
    q:               str        = Query(..., min_length=1, description="Raw product name to harmonise"),
    k:               int        = Query(5,  ge=1, le=20, description="Number of candidates to return"),
    min_confidence:  Confidence = Query(Confidence.LOW, description="Minimum confidence bucket"),
):
    return services.harmonise_product(q, k, min_confidence)


# ===========================================================================
# Bad-records review workflow (supporting /compute-dq output)
# ===========================================================================

@app.get(
    "/bad-records",
    tags=["business-review"],
    response_model=BadRecordList,
    summary="List records flagged by Data Quality rules",
    description=(
        "Powers the business-user review UI. Default filter: `status=NEW` "
        "to surface unresolved items. Pagination via `page` / `page_size`."
    ),
)
async def list_bad_records(
    status:    BadRecordStatus = Query(BadRecordStatus.NEW),
    severity:  Severity        = Query(None),
    assignee:  str             = Query(None),
    page:      int             = Query(1, ge=1),
    page_size: int             = Query(50, ge=1, le=500),
):
    total, items = await services.list_bad_records(status, severity, assignee, page, page_size)
    return BadRecordList(total=total, page=page, page_size=page_size, items=items)


@app.post(
    "/bad-records/{bad_record_id}/resolve",
    tags=["business-review"],
    response_model=ResolveBadRecordResponse,
    summary="Mark a bad record as resolved (or ignored)",
    description=(
        "Triggers the business-correction loop. When `replay_batch=true`, the "
        "associated `source_batch_id` is re-ingested with the updated "
        "abbreviation dictionary / rules — only that batch is re-processed."
    ),
)
async def resolve_bad_record(
    bad_record_id: int,
    req:           ResolveBadRecordRequest,
):
    new_status, resolved_at, replay = await services.resolve_bad_record(
        bad_record_id, req.action, req.notes, req.replay_batch
    )
    return ResolveBadRecordResponse(
        bad_record_id    = bad_record_id,
        new_status       = new_status,
        resolved_at      = resolved_at,
        replay_triggered = replay,
    )
