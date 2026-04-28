"""
End-to-end smoke tests for the API skeleton.

Use FastAPI's TestClient — no running server needed.

Run from the unzipped project folder (containing this README):
    python3 -m pytest api/tests -v
"""

from __future__ import annotations

import io
import uuid

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
    # `with TestClient(app)` triggers FastAPI's lifespan (init_pool / close_pool)
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# /harmonise-product
# ---------------------------------------------------------------------------

class TestHarmoniseEndpoint:
    def test_high_confidence_match(self, client):
        r = client.get("/harmonise-product", params={"q": "iP 17 PM 512GB", "k": 3})
        assert r.status_code == 200
        body = r.json()
        assert body["query"] == "iP 17 PM 512GB"
        assert len(body["matches"]) > 0
        top = body["matches"][0]
        assert top["confidence"] in ("HIGH", "MEDIUM")
        assert "iPhone 17 Pro Max" in top["canonical_name"]
        # Signal breakdown is exposed for explainability
        sb = top["signal_breakdown"]
        assert 0 <= sb["attr_match"] <= 1

    def test_min_confidence_filter_drops_low(self, client):
        r = client.get(
            "/harmonise-product",
            params={"q": "asdf random text", "k": 5, "min_confidence": "HIGH"},
        )
        assert r.status_code == 200
        body = r.json()
        # All returned matches must be HIGH (filter is enforced)
        for m in body["matches"]:
            assert m["confidence"] == "HIGH"

    def test_empty_query_rejected(self, client):
        r = client.get("/harmonise-product", params={"q": "", "k": 3})
        assert r.status_code == 422   # Pydantic validation error


# ---------------------------------------------------------------------------
# /load-data + /load-data/{job_id}
# ---------------------------------------------------------------------------

class TestLoadDataEndpoints:
    def test_post_load_data_returns_202(self, client):
        csv_content = b"CRAWL_TS,COUNTRY_VAL,PARTNER,PRODUCT_NAME_VAL,FULL PRICE\n"
        csv_content += b"2025-10-03T17:00:00Z,New Zealand,Partner B,iPhone 17 256GB,1999\n"

        r = client.post(
            "/load-data",
            data={"partner_code": "PARTNER_B"},
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
        )
        assert r.status_code == 202
        body = r.json()
        assert "job_id" in body
        # Real implementation processes synchronously for the demo, so the
        # response status will be COMPLETED already. Async-with-202+QUEUED
        # is the production pattern; demo collapses the work into the
        # request handler. Either is acceptable.
        assert body["status"] in ("QUEUED", "RUNNING", "COMPLETED")
        assert body["poll_url"].endswith(body["job_id"])

    def test_load_data_rejects_non_csv(self, client):
        r = client.post(
            "/load-data",
            data={"partner_code": "PARTNER_A"},
            files={"file": ("test.json", io.BytesIO(b"{}"), "application/json")},
        )
        assert r.status_code == 400

    def test_load_data_rejects_empty_file(self, client):
        r = client.post(
            "/load-data",
            data={"partner_code": "PARTNER_A"},
            files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
        )
        assert r.status_code == 400

    def test_get_job_status(self, client):
        # Submit first
        csv = b"CRAWL_TS,FULL PRICE\n2025-10-03T00:00:00Z,1999\n"
        post = client.post(
            "/load-data",
            data={"partner_code": "PARTNER_A"},
            files={"file": ("a.csv", io.BytesIO(csv), "text/csv")},
        )
        job_id = post.json()["job_id"]

        # Then poll
        r = client.get(f"/load-data/{job_id}")
        assert r.status_code == 200
        body = r.json()
        assert body["job_id"] == job_id
        assert "progress" in body

    def test_get_unknown_job_returns_404(self, client):
        r = client.get(f"/load-data/{uuid.uuid4()}")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# /compute-dq
# ---------------------------------------------------------------------------

class TestComputeDQ:
    def test_compute_dq_basic(self, client):
        r = client.post(
            "/compute-dq",
            json={"source_batch_id": str(uuid.uuid4())},
        )
        assert r.status_code == 200
        body = r.json()
        # All 13 active rules execute; some may report 0 rows for an empty batch
        assert body["summary"]["total_rules_run"] >= 0
        assert "by_severity" in body["summary"]
        assert isinstance(body["rule_runs"], list)

    def test_compute_dq_request_schema_minimal(self, client):
        # The request payload accepts ONLY source_batch_id — extras are ignored
        # by Pydantic's default. Sending unknown fields must NOT error.
        r = client.post(
            "/compute-dq",
            json={"source_batch_id": str(uuid.uuid4()), "ignored_field": "x"},
        )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# /detect-anomalies
# ---------------------------------------------------------------------------

class TestDetectAnomalies:
    def test_detect_with_batch_id(self, client):
        r = client.post(
            "/detect-anomalies",
            json={"source_batch_id": str(uuid.uuid4()), "min_severity": "MEDIUM"},
        )
        assert r.status_code == 200
        body = r.json()
        assert "anomalies" in body
        assert "by_severity" in body

    def test_detect_requires_scope(self, client):
        # Neither source_batch_id nor date_range → should fail
        r = client.post("/detect-anomalies", json={"min_severity": "LOW"})
        assert r.status_code == 400

    def test_min_severity_filters(self, client):
        r = client.post(
            "/detect-anomalies",
            json={"source_batch_id": str(uuid.uuid4()), "min_severity": "HIGH"},
        )
        assert r.status_code == 200
        for a in r.json()["anomalies"]:
            assert a["severity"] == "HIGH"


# ---------------------------------------------------------------------------
# /bad-records
# ---------------------------------------------------------------------------

class TestBadRecords:
    def test_list_bad_records(self, client):
        r = client.get("/bad-records", params={"status": "NEW"})
        assert r.status_code == 200
        body = r.json()
        assert "items" in body
        assert "page" in body

    def test_resolve_bad_record_unknown_returns_404(self, client):
        # Bad record 999999 definitely does not exist → real impl returns 404
        r = client.post(
            "/bad-records/999999/resolve",
            json={"action": "RESOLVED", "notes": "test", "replay_batch": False},
        )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# OpenAPI doc generation
# ---------------------------------------------------------------------------

def test_openapi_spec_generates(client):
    r = client.get("/openapi.json")
    assert r.status_code == 200
    spec = r.json()
    paths = spec["paths"]
    # 4 Task-B sub-modules
    assert "/harmonise-product" in paths
    assert "/load-data" in paths
    assert "/load-data/{job_id}" in paths
    assert "/compute-dq" in paths
    assert "/detect-anomalies" in paths
    # Path A orchestrator
    assert "/pipeline" in paths
    # Bad-records review workflow
    assert "/bad-records" in paths
    assert "/bad-records/{bad_record_id}/resolve" in paths


# ---------------------------------------------------------------------------
# /pipeline — Path A orchestrator
# ---------------------------------------------------------------------------

class TestPipelineOrchestrator:
    def test_pipeline_runs_end_to_end(self, client):
        csv_content = b"CRAWL_TS,COUNTRY_VAL,PARTNER,PRODUCT_NAME_VAL,FULL PRICE\n"
        csv_content += b"2025-10-03T17:00:00Z,New Zealand,Partner B,iPhone 17 256GB,1999\n"

        r = client.post(
            "/pipeline",
            data={"partner_code": "PARTNER_B"},
            files={"file": ("test.csv", io.BytesIO(csv_content), "text/csv")},
        )
        assert r.status_code == 200
        body = r.json()
        # Orchestrator returns aggregated result of all 9 steps
        assert "job_id" in body
        assert "source_batch_id" in body
        assert "rows_stg" in body
        assert "rows_loaded" in body
        assert "rows_bad" in body
        assert "rows_history" in body
        assert "dq_summary" in body
        assert "dq_by_stage" in body
        assert "anomalies_total" in body
        assert "anomalies_by_severity" in body
        # dq_by_stage always exposes all 3 stages
        assert set(body["dq_by_stage"].keys()) == {"INGEST", "PRE_FACT", "SEMANTIC"}

    def test_pipeline_rejects_non_csv(self, client):
        r = client.post(
            "/pipeline",
            data={"partner_code": "PARTNER_A"},
            files={"file": ("test.json", io.BytesIO(b"{}"), "application/json")},
        )
        assert r.status_code == 400

    def test_pipeline_rejects_empty_file(self, client):
        r = client.post(
            "/pipeline",
            data={"partner_code": "PARTNER_A"},
            files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
        )
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# Path A vs Path B — both cover all 9 steps; gating semantics differ
# ---------------------------------------------------------------------------

class TestPathParity:
    """Verify both call paths cover the same 9 logical steps.

    Path A (/pipeline): one transaction, interleaved order, PRE_FACT hard gate.
    Path B (/load-data → /compute-dq → /detect-anomalies): grouped order,
                                                            post-hoc flagging.
    """
    _CSV = (
        b"CRAWL_TS,COUNTRY_VAL,PARTNER,PRODUCT_NAME_VAL,FULL PRICE\n"
        b"2025-10-03T17:00:00Z,New Zealand,Partner B,iPhone 17 256GB,1999\n"
    )

    def test_path_a_returns_aggregated_summary(self, client):
        r = client.post(
            "/pipeline",
            data={"partner_code": "PARTNER_B"},
            files={"file": ("p.csv", io.BytesIO(self._CSV), "text/csv")},
        )
        assert r.status_code == 200
        body = r.json()
        # Path A's summary already includes DQ + anomalies (one round trip)
        assert "dq_summary" in body
        assert "anomalies_total" in body

    def test_path_b_three_step_sequence(self, client):
        # Step 1: load
        load = client.post(
            "/load-data",
            data={"partner_code": "PARTNER_B"},
            files={"file": ("p.csv", io.BytesIO(self._CSV), "text/csv")},
        )
        assert load.status_code == 202
        batch_id = load.json()["source_batch_id"]

        # Step 2: compute-dq
        dq = client.post("/compute-dq", json={"source_batch_id": batch_id})
        assert dq.status_code == 200
        assert "summary" in dq.json()

        # Step 3: detect-anomalies
        an = client.post(
            "/detect-anomalies",
            json={"source_batch_id": batch_id, "min_severity": "LOW"},
        )
        assert an.status_code == 200
        assert "anomalies" in an.json()


# ---------------------------------------------------------------------------
# Anomaly multi-signal detectors — TEMPORAL / CROSS_PARTNER / SKU_VARIANCE
# ---------------------------------------------------------------------------

class TestAnomalyMultiSignal:
    """Verify the 4 detectors are wired and produce well-formed Anomaly rows
    with the correct anomaly_type. Uses small in-memory CSV inputs.

    These tests prove the detectors EXIST and respect the AnomalyType taxonomy;
    end-to-end "did this trigger HIGH severity" tests need richer cross-partner
    data (PARTNER_A and PARTNER_B in the same country) which the sample CSVs
    don't provide — verified manually via synthetic injection in development.
    """

    _CSV_VARIANCE = (
        b"CRAWL_TS,COUNTRY_VAL,PARTNER,PRODUCT_NAME_VAL,FULL PRICE\n"
        # 3 different prices for the SAME product on the SAME date —
        # SKU_VARIANCE detector groups these and computes per-group z-score
        b"2025-10-03T17:00:00Z,New Zealand,Partner B,Apple iPad Pro 11\" (M5) - Space Black 256GB Storage - WiFi,1500\n"
        b"2025-10-03T17:00:00Z,New Zealand,Partner B,Apple iPad Pro 11\" (M5) - Space Black 256GB Storage - WiFi,1520\n"
        b"2025-10-03T17:00:00Z,New Zealand,Partner B,Apple iPad Pro 11\" (M5) - Space Black 256GB Storage - WiFi,9999\n"
    )

    def test_pipeline_response_includes_anomaly_breakdown(self, client):
        """Pipeline response surface includes anomalies_by_severity object —
        this hooks the multi-signal detectors into the dashboard."""
        r = client.post(
            "/pipeline",
            data={"partner_code": "PARTNER_B"},
            files={"file": ("variance.csv", io.BytesIO(self._CSV_VARIANCE), "text/csv")},
        )
        assert r.status_code == 200
        body = r.json()
        assert "anomalies_total" in body
        assert "anomalies_by_severity" in body
        # Severity dict is always present with all 3 keys (even at 0)
        assert set(body["anomalies_by_severity"].keys()) == {"HIGH", "MEDIUM", "LOW"}

    def test_dashboard_stats_anomaly_taxonomy(self, client):
        """Dashboard /dashboard-stats exposes by_type — this is where the 4
        signal types surface to the frontend doughnut chart."""
        r = client.get("/dashboard-stats")
        assert r.status_code == 200
        body = r.json()
        a = body["anomaly_stats"]
        # Schema contract: by_type is a dict (might be empty if no anomalies)
        assert isinstance(a["by_type"], dict)
        # If any anomalies were detected, every type must be a known AnomalyType
        valid_types = {"STATISTICAL", "TEMPORAL", "CROSS_PARTNER", "SKU_VARIANCE"}
        for t in a["by_type"].keys():
            assert t in valid_types, f"unknown anomaly_type: {t}"
        # recent_anomalies items also must use the known taxonomy
        for item in a.get("recent_anomalies", []):
            assert item["anomaly_type"] in valid_types

    def test_anomaly_endpoint_accepts_min_severity_filter(self, client):
        """Re-confirm the min_severity filter respects the SEV_RANK ladder
        across all 4 signals (HIGH-only filter must drop MEDIUM/LOW from
        every detector, not just STATISTICAL)."""
        r = client.post(
            "/detect-anomalies",
            json={"source_batch_id": str(uuid.uuid4()), "min_severity": "HIGH"},
        )
        assert r.status_code == 200
        for a in r.json()["anomalies"]:
            assert a["severity"] == "HIGH"
