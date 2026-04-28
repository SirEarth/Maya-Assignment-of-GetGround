"""
Pydantic (data validation library) models — request and response schemas.

All endpoints reference these models so the API contract is explicit and
auto-documented in the generated OpenAPI specification.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enumerations (mirror the ENUM types in schema.sql)
# ---------------------------------------------------------------------------

class Confidence(str, Enum):
    HIGH   = "HIGH"
    MEDIUM = "MEDIUM"
    LOW    = "LOW"
    MANUAL = "MANUAL"


class Severity(str, Enum):
    HIGH   = "HIGH"
    MEDIUM = "MEDIUM"
    LOW    = "LOW"


class JobStatus(str, Enum):
    QUEUED    = "QUEUED"
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    PARTIAL   = "PARTIAL"


class AnomalyType(str, Enum):
    """
    Signal taxonomy. Anomaly detection answers "is this price unusual *relative*
    to other observations" — it deliberately does NOT subsume DQ rule violations
    (e.g. DQ_PRICE_001's per-category band check, which catches absolute-typo
    errors). DQ and Anomaly are complementary mechanisms with separate review
    paths (`/bad-records` vs `/detect-anomalies`).

    All four signals are implemented; each has its own detector helper in
    `api/services.py` and writes its own row to `fact_anomaly` (UNIQUE on
    (offer_id, anomaly_type)). Severity is per-signal — independent severity
    routing means HIGH on one signal doesn't dilute MEDIUM on another.
    """
    STATISTICAL      = "STATISTICAL"      # vs 30-day rolling baseline (mean / stddev from SCD-2 history)
    TEMPORAL         = "TEMPORAL"         # vs the immediate previous price for the same key
    CROSS_PARTNER    = "CROSS_PARTNER"    # vs peer-partner median via v_partner_price_current
    SKU_VARIANCE     = "SKU_VARIANCE"     # z-score within same-model same-day same-partner group


class LoadMode(str, Enum):
    """Sync only acceptable for small files (< 1k rows). Default async."""
    ASYNC = "async"
    SYNC  = "sync"


# ---------------------------------------------------------------------------
# /harmonise-product
# ---------------------------------------------------------------------------

class SignalBreakdown(BaseModel):
    """Per-signal scoring detail — matches scorer.ScoreBreakdown."""
    attr_match:       float = Field(..., description="Structured attribute match score (0-1)")
    token_jaccard:    float = Field(..., description="Token set Jaccard similarity (0-1)")
    char_fuzz:        float = Field(..., description="Character-level fuzzy ratio (0-1)")
    attr_matched:     List[str] = Field(default_factory=list, description="Attributes matched at >= 0.8 similarity")
    attr_mismatched:  List[str] = Field(default_factory=list, description="Attributes mismatched at < 0.3 similarity")


class HarmoniseMatch(BaseModel):
    """One candidate match returned by /harmonise-product."""
    model_key:        str           = Field(..., description="Canonical model identifier")
    canonical_name:   str           = Field(..., description="Human-readable product name")
    sku_ids:          List[int]     = Field(..., description="Original Product Ref IDs grouped under this model (color variants)")
    score:            float         = Field(..., ge=0, le=1, description="Combined match score (0-1)")
    confidence:       Confidence    = Field(..., description="HIGH/MEDIUM/LOW/MANUAL bucket")
    signal_breakdown: SignalBreakdown


class HarmoniseResponse(BaseModel):
    """Response from GET /harmonise-product."""
    query:      str                  = Field(..., description="The product name as submitted")
    matches:    List[HarmoniseMatch] = Field(..., description="Top-K candidate matches, ranked descending by score")
    elapsed_ms: int                  = Field(..., description="Server processing time")


# ---------------------------------------------------------------------------
# /load-data
# ---------------------------------------------------------------------------

class LoadDataAcceptedResponse(BaseModel):
    """HTTP 202 Accepted — async ingest queued."""
    job_id:           UUID
    source_batch_id:  UUID
    status:           JobStatus = JobStatus.QUEUED
    poll_url:         str
    submitted_at:     datetime


class LoadDataSyncSummary(BaseModel):
    """HTTP 200 OK — sync mode small-file response."""
    source_batch_id:  UUID
    status:           JobStatus = JobStatus.COMPLETED
    rows_loaded:      int
    rows_bad:         int
    harmonise_high_pct: float    = Field(..., ge=0, le=1)


class LoadDataProgress(BaseModel):
    chunk_count:        int = Field(..., description="Total chunks the file was split into")
    completed_chunks:   int
    rows_loaded:        int
    rows_bad:           int
    percent_complete:   float = Field(..., ge=0, le=100)


class LoadJobStatus(BaseModel):
    """Response for GET /load-data/{job_id}."""
    job_id:                  UUID
    source_batch_id:         UUID
    status:                  JobStatus
    progress:                LoadDataProgress
    started_at:              Optional[datetime] = None
    estimated_completion_at: Optional[datetime] = None
    error_message:           Optional[str]      = None


# ---------------------------------------------------------------------------
# /compute-dq
# ---------------------------------------------------------------------------

class DQRuleRunResult(BaseModel):
    """One rule's run statistics — matches dq_output schema."""
    rule_id:        str
    rule_name:      str
    rule_category:  str
    severity:       Severity
    target_stage:   str   = Field(..., description="INGEST, PRE_FACT, or SEMANTIC")
    total_records:  int
    failed_records: int
    pass_rate:      float = Field(..., ge=0, le=1)


class DQViolationsBySeverity(BaseModel):
    HIGH:   int = 0
    MEDIUM: int = 0
    LOW:    int = 0


class DQSummary(BaseModel):
    total_rules_run:   int
    total_records:     int = Field(..., description="Records examined (max across rules)")
    total_violations:  int
    by_severity:       DQViolationsBySeverity


class ComputeDQRequest(BaseModel):
    source_batch_id: UUID = Field(..., description="Run all 13 DQ rules against this ingestion batch")


class ComputeDQResponse(BaseModel):
    source_batch_id: UUID
    started_at:      datetime
    completed_at:    datetime
    summary:         DQSummary
    rule_runs:       List[DQRuleRunResult]


# ---------------------------------------------------------------------------
# /detect-anomalies
# ---------------------------------------------------------------------------

class DateRange(BaseModel):
    from_date: date = Field(..., alias="from")
    to_date:   date = Field(..., alias="to")
    model_config = ConfigDict(populate_by_name=True)


class DetectAnomaliesRequest(BaseModel):
    source_batch_id:    Optional[UUID]      = Field(default=None, description="Detect against this batch's offers (preferred)")
    date_range:         Optional[DateRange] = Field(default=None, description="Alternative: detect over a date range")
    product_model_ids:  Optional[List[int]] = Field(default=None, description="Filter to specific products")
    min_severity:       Severity            = Field(default=Severity.LOW, description="Minimum severity to return")


class BaselineSnapshot(BaseModel):
    """Frozen snapshot of dws_product_price_baseline_1d at detection time."""
    window_days:  int
    sample_size:  int
    mean:         float
    stddev:       Optional[float] = None
    p05:          Optional[float] = None
    p50:          Optional[float] = None
    p95:          Optional[float] = None


class AnomalyContext(BaseModel):
    lifecycle_status:         str
    lifecycle_factor:         float
    event_suppression_factor: float = 1.0
    suppression_applied:      bool  = False
    suppression_event_id:     Optional[int]      = None
    baseline_snapshot:        Optional[BaselineSnapshot] = None


class TimeSeriesPoint(BaseModel):
    date:        date
    price_usd:   float
    is_anomaly:  bool = False


class AnomalyVisualization(BaseModel):
    """Structured payload for frontend chart libraries (Chart.js / Recharts)."""
    type:                       str = Field("time_series_with_band")
    series:                     List[TimeSeriesPoint]
    baseline_band:              Optional[Dict[str, float]] = None
    cross_partner_comparison:   Optional[Dict[str, float]] = None


class Anomaly(BaseModel):
    anomaly_id:           int
    offer_id:             int
    anomaly_type:         AnomalyType
    severity:             Severity
    signal_score:         float    = Field(..., ge=0, le=1)
    product_model_id:     int
    product_model_name:   str
    partner_code:         str
    country_code:         str
    observed_price_usd:   float
    explanation:          str
    context:              AnomalyContext
    visualization:        Optional[AnomalyVisualization] = None
    detected_at:          datetime


class AnomaliesBySeverity(BaseModel):
    HIGH:   int = 0
    MEDIUM: int = 0
    LOW:    int = 0


class DetectAnomaliesResponse(BaseModel):
    detected_at:        datetime
    total_anomalies:    int
    by_severity:        AnomaliesBySeverity
    anomalies:          List[Anomaly]


# ---------------------------------------------------------------------------
# /pipeline — Path A orchestrator (one-click 9-step end-to-end)
# ---------------------------------------------------------------------------

class PipelineResponse(BaseModel):
    """Aggregated result of the 9-step orchestrator.

    The orchestrator interleaves the 4 sub-modules' work in the canonical
    pipeline order (parse → ingest_dq → harmonise → prefact_dq → fact_write
    with hard gate → semantic_dq → scd2 → anomaly_detection → summary).
    Sub-module endpoints can replicate steps 1-9 in grouped fashion (see
    /load-data, /compute-dq, /detect-anomalies); the orchestrator exists
    for a one-shot call with strict PRE_FACT gating semantics.
    """
    job_id:                UUID
    source_batch_id:       UUID
    partner_code:          str
    started_at:            datetime
    completed_at:          datetime
    rows_stg:              int                  = Field(..., description="Rows in stg_price_offer for this batch (= rows parsed)")
    rows_loaded:           int                  = Field(..., description="Rows in fact_price_offer for this batch")
    rows_unchanged:        int                  = Field(..., description="Rows neither loaded to fact nor flagged bad")
    rows_bad:              int                  = Field(..., description="Rows in dq_bad_records for this batch")
    rows_history:          int                  = Field(..., description="Rows added/updated in fact_partner_price_history")
    dq_summary:            DQSummary
    dq_by_stage:           Dict[str, int]       = Field(default_factory=dict, description="DQ failed-record counts per stage: INGEST / PRE_FACT / SEMANTIC")
    anomalies_total:       int
    anomalies_by_severity: AnomaliesBySeverity


# ---------------------------------------------------------------------------
# /dashboard-stats — DB-wide aggregates for the live visualization dashboard
# ---------------------------------------------------------------------------

class DQRulePassRate(BaseModel):
    """One rule's aggregated pass rate across all batches in the database."""
    rule_id:        str
    rule_name:      str
    rule_category:  str
    severity:       Severity
    target_stage:   str
    description:    Optional[str] = Field(default=None, description="Human-readable rule description from dq_rule_catalog.description")
    total_records:  int
    failed_records: int
    pass_rate:      float = Field(..., ge=0, le=1)


class DashboardTotals(BaseModel):
    rows_stg:                int
    rows_fact:               int
    rows_history:            int
    rows_bad:                int
    rows_blocked_by_prefact: int   = Field(..., description="Distinct rows blocked at the PRE_FACT gate (across all batches)")
    harmonise_high_pct:      float = Field(..., ge=0, le=1, description="Share of fact rows with HIGH harmonise confidence")
    total_violations:        int   = Field(..., description="Same as rows_bad — kept for headline-card naming clarity")
    batches_loaded:          int
    rules_in_catalog:        int


class FunnelStats(BaseModel):
    stg:             int
    ingest_passed:   int   = Field(..., description="stg rows with dq_status IN ('INGEST_PASSED','PRE_FACT_PASSED')")
    after_harmonise: int   = Field(..., description="stg rows with product_model_id NOT NULL")
    prefact_passed:  int   = Field(..., description="stg rows with dq_status = 'PRE_FACT_PASSED'")
    fact:            int
    history:         int


class HarmoniseSample(BaseModel):
    """One representative harmonise example with the full 3-signal breakdown,
    designed for the showcase 'Real Match Examples' card on the dashboard."""
    raw_product_name:   str
    canonical_name:     str
    model_key:          str
    sku_ids:            List[int]   = Field(default_factory=list)
    score:              float       = Field(..., ge=0, le=1)
    confidence:         Confidence
    signal_breakdown:   SignalBreakdown
    note:               str         = Field(default="", description="Why this example is interesting (e.g. 'structural override fired')")


class AnomalyDashboardStats(BaseModel):
    """Anomaly detection summary across recent batches + one illustrative
    visualization + per-signal breakdown + a list of the most-severe recent
    anomalies for the dashboard table.

    `sample_visualization` is always a STATISTICAL-style 30-day time-series
    with ±1σ band: it shows the absolute top anomaly when that's STATISTICAL,
    falls back to the highest STATISTICAL anomaly in the recent window when
    the top is point-in-time (TEMPORAL / CROSS_PARTNER / SKU_VARIANCE), and
    falls back to a baseline-only illustration from the richest-history
    product when no STATISTICAL anomaly exists. `sample_caption` records
    which path produced the chart.
    """
    total_detected:        int
    by_severity:           AnomaliesBySeverity
    by_type:               Dict[str, int] = Field(default_factory=dict, description="Anomaly counts per AnomalyType — all 4 signals (STATISTICAL / TEMPORAL / CROSS_PARTNER / SKU_VARIANCE)")
    sample_visualization:  Optional[AnomalyVisualization] = None
    sample_caption:        str  = Field(default="")
    sample_severity:       Optional[Severity] = None
    is_real_anomaly:       bool = Field(default=False, description="True if sample is a real triggered anomaly; False if showing baseline-only illustration")
    recent_anomalies:      List[Anomaly] = Field(default_factory=list, description="Top-N most severe anomalies (sorted by severity then signal_score) for the dashboard table")


class DashboardStats(BaseModel):
    """DB-wide aggregate snapshot powering the live visualization dashboard.

    Reads across ALL batches currently in the database — not scoped to a
    single source_batch_id. Refreshes whenever the frontend polls the
    GET /dashboard-stats endpoint.
    """
    snapshot_at:                       datetime
    totals:                            DashboardTotals
    harmonise_confidence:              Dict[str, int]      = Field(..., description="Counts per HIGH/MEDIUM/LOW/MANUAL across fact_price_offer")
    dq_violations_by_severity:         DQViolationsBySeverity
    dq_violations_by_stage:            Dict[str, int]      = Field(..., description="Bad-record counts by INGEST/PRE_FACT/SEMANTIC stage")
    dq_pass_rates:                     List[DQRulePassRate]
    funnel:                            FunnelStats
    sample_bad_records:                List[BadRecordEntry] = Field(..., description="Recent N bad records (any rule)")
    prefact_blocked_records:           List[BadRecordEntry] = Field(..., description="Full distinct list of rows blocked by the PRE_FACT gate (1 row per offending stg row, even if it failed multiple PRE_FACT rules)")
    low_confidence_harmonise_records:  List[BadRecordEntry] = Field(..., description="Full list of DQ_HARM_001 (low-confidence harmonise) records — flagged-and-kept under SEMANTIC policy")
    harmonise_samples:                 List[HarmoniseSample] = Field(..., description="Stratified harmonise examples with signal breakdown for the showcase 'Real Match Examples' card")
    anomaly_stats:                     AnomalyDashboardStats


# ---------------------------------------------------------------------------
# /bad-records (business review workflow — Task C-2)
# ---------------------------------------------------------------------------

class BadRecordStatus(str, Enum):
    NEW        = "NEW"
    IN_REVIEW  = "IN_REVIEW"
    RESOLVED   = "RESOLVED"
    IGNORED    = "IGNORED"


class BadRecordEntry(BaseModel):
    bad_record_id:    int
    source_batch_id:  UUID
    rule_id:          str
    target_stage:     Optional[str]      = Field(default=None, description="INGEST/PRE_FACT/SEMANTIC — looked up from dq_rule_catalog")
    failed_field:     Optional[str]
    error_message:    str
    severity:         Severity
    status:           BadRecordStatus
    assignee:         Optional[str]      = None
    raw_payload:      Dict[str, Any]
    detected_at:      datetime
    resolved_at:      Optional[datetime] = None


class BadRecordList(BaseModel):
    total:       int
    page:        int
    page_size:   int
    items:       List[BadRecordEntry]


class ResolveAction(str, Enum):
    RESOLVED  = "RESOLVED"
    IGNORED   = "IGNORED"


class ResolveBadRecordRequest(BaseModel):
    action:        ResolveAction
    notes:         Optional[str]  = None
    replay_batch:  bool           = Field(default=False, description="Re-ingest the batch with corrected rules / dictionary after resolving")


class ResolveBadRecordResponse(BaseModel):
    bad_record_id:    int
    new_status:       BadRecordStatus
    resolved_at:      datetime
    replay_triggered: bool = False


# ---------------------------------------------------------------------------
# Generic
# ---------------------------------------------------------------------------

class ErrorResponse(BaseModel):
    error:   str
    detail:  Optional[str] = None
    code:    Optional[str] = None
