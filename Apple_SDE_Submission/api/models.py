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
    Designed signal taxonomy. Current implementation only emits `STATISTICAL`
    (compares observed price against the 30-day rolling mean from
    `fact_partner_price_history`). The other three are scoped as future work;
    `fact_anomaly` already has columns to hold their results.
    """
    STATISTICAL    = "STATISTICAL"   # implemented
    TEMPORAL       = "TEMPORAL"      # design only — would compare to last valid price in SCD-2 history
    CROSS_PARTNER  = "CROSS_PARTNER" # design only — would compare to other partners via v_partner_price_current
    SKU_VARIANCE   = "SKU_VARIANCE"  # design only — would compute price spread across SKUs of same model


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
    source_batch_id: UUID                  = Field(..., description="Run DQ checks against this ingestion batch")
    rules:           Optional[List[str]]   = Field(default=None, description="Optional whitelist of rule_ids; default = all active")
    stages:          Optional[List[str]]   = Field(default=None, description="Optional subset of INGEST / PRE_FACT / SEMANTIC; default = all three")


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
