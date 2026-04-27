"""
FastAPI application — entry point.

All four assignment endpoints + supporting endpoints for the async load
workflow and business-user bad-records review.

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
from fastapi.responses import JSONResponse

from . import services
from .db import close_pool, init_pool
from .models import (
    BadRecordList, BadRecordStatus, ComputeDQRequest, ComputeDQResponse,
    Confidence, DetectAnomaliesRequest, DetectAnomaliesResponse,
    ErrorResponse, HarmoniseResponse, LoadDataAcceptedResponse, LoadJobStatus,
    ResolveBadRecordRequest, ResolveBadRecordResponse, Severity,
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
        "API (Application Programming Interface) for the partner-store pricing "
        "ingestion + analysis system."
    ),
    version="0.1.0",
    contact={"name": "huizhongwu"},
    lifespan=lifespan,
)


# OPTIONAL — not required by the assignment. Standard production practice
# for container-orchestration platforms (Kubernetes / ECS / ALB) which call
# this endpoint to decide whether the service is alive and should receive
# traffic. Safe to remove if not deploying to such a platform.
@app.get("/health", tags=["meta"], summary="Liveness probe")
def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# /harmonise-product — assignment endpoint #4
# ---------------------------------------------------------------------------

@app.get(
    "/harmonise-product",
    tags=["harmonise"],
    response_model=HarmoniseResponse,
    summary="Harmonise a raw product name to canonical model(s)",
    description=(
        "Returns up to **k** ranked candidate matches for a partner-supplied "
        "product name. Each candidate has a 0-1 score, a confidence bucket "
        "(HIGH / MEDIUM / LOW / MANUAL) and a per-signal breakdown for "
        "explainability."
    ),
)
def harmonise_product(
    q:               str        = Query(..., min_length=1, description="Raw product name to harmonise"),
    k:               int        = Query(5,  ge=1, le=20, description="Number of candidates to return"),
    min_confidence:  Confidence = Query(Confidence.LOW, description="Minimum confidence bucket"),
):
    return services.harmonise_product(q, k, min_confidence)


# ---------------------------------------------------------------------------
# /load-data — assignment endpoint #1 (async-first)
# ---------------------------------------------------------------------------

@app.post(
    "/load-data",
    tags=["ingest"],
    status_code=status.HTTP_202_ACCEPTED,
    response_model=LoadDataAcceptedResponse,
    responses={400: {"model": ErrorResponse}},
    summary="Submit a CSV (Comma-Separated Values) batch and run the full ingest pipeline",
    description=(
        "Accepts a multipart upload and runs the eight-step ingest pipeline "
        "in-process:\n\n"
        "  1. Parse CSV into `stg_price_offer`\n"
        "  2. Run INGEST-stage Data Quality rules (parse / format / required-field)\n"
        "  3. Harmonise raw product names against the canonical registry\n"
        "  4. Run PRE_FACT-stage Data Quality rules (HIGH-severity gate); failing "
        "rows are flagged in `dq_bad_records` and do NOT enter fact tables\n"
        "  5. Insert change events into `fact_price_offer` + payment child "
        "(only PRE_FACT-passing rows)\n"
        "  6. Run SEMANTIC-stage Data Quality rules on fact rows (soft signals; "
        "failures stay in fact and are flagged for review)\n"
        "  7. Update Slowly Changing Dimension Type 2 history\n"
        "  8. Write per-batch summary to `dws_partner_dq_per_batch`\n\n"
        "Returns `HTTP 202 Accepted` with a `job_id`. At sample-data scale "
        "(~4 000 rows) processing completes in seconds, so the response "
        "status is `COMPLETED` on return. The full async-queue + parallel-"
        "worker + AWS S3 (Amazon Simple Storage Service) staging path for "
        "million-row scale is the production design (see `task_c_answers.md` C.3)."
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
    tags=["ingest"],
    response_model=LoadJobStatus,
    responses={404: {"model": ErrorResponse}},
    summary="Poll the progress / status of a load job",
)
def get_load_job(
    job_id: UUID = Path(..., description="The job_id returned by POST /load-data"),
):
    return services.get_job_status(job_id)


# ---------------------------------------------------------------------------
# /compute-dq — assignment endpoint #2
# ---------------------------------------------------------------------------

@app.post(
    "/compute-dq",
    tags=["data-quality"],
    response_model=ComputeDQResponse,
    summary="Run Data Quality (DQ) rules against an ingestion batch",
    description=(
        "Executes all active rules in `dq_rule_catalog` against the specified "
        "batch. Aggregates per-rule pass rates into `dq_output` and per-record "
        "violations into `dq_bad_records`.\n\n"
        "Optionally restrict to specific `rules` and/or `stages` "
        "(INGEST / PRE_FACT / SEMANTIC)."
    ),
)
async def compute_dq(req: ComputeDQRequest):
    started = datetime.now(timezone.utc)
    summary, rule_runs = await services.compute_dq(req.source_batch_id, req.rules, req.stages)
    completed = datetime.now(timezone.utc)
    return ComputeDQResponse(
        source_batch_id = req.source_batch_id,
        started_at      = started,
        completed_at    = completed,
        summary         = summary,
        rule_runs       = rule_runs,
    )


# ---------------------------------------------------------------------------
# /detect-anomalies — assignment endpoint #3
# ---------------------------------------------------------------------------

@app.post(
    "/detect-anomalies",
    tags=["anomaly-detection"],
    response_model=DetectAnomaliesResponse,
    summary="Detect pricing anomalies",
    description=(
        "Runs the four-signal detector (STATISTICAL / TEMPORAL / "
        "CROSS_PARTNER / SKU_VARIANCE) over the specified scope.\n\n"
        "Each row in the response represents a single triggered signal — an "
        "offer that trips multiple signals appears as multiple anomaly "
        "entries, each routable to the appropriate team (see "
        "`fact_anomaly` in schema.sql).\n\n"
        "All thresholds are read from `dim_anomaly_threshold` and frozen "
        "into the response context for replay/audit."
    ),
)
async def detect_anomalies(req: DetectAnomaliesRequest):
    if not req.source_batch_id and not req.date_range:
        raise HTTPException(400, "Must specify either source_batch_id or date_range")
    return await services.detect_anomalies(req)


# ---------------------------------------------------------------------------
# /bad-records — business-user review workflow (Task C-2 hook)
# ---------------------------------------------------------------------------

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
