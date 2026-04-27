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
        assert body["summary"]["total_rules_run"] > 0
        assert "by_severity" in body["summary"]
        assert isinstance(body["rule_runs"], list)

    def test_compute_dq_with_rule_filter(self, client):
        r = client.post(
            "/compute-dq",
            json={
                "source_batch_id": str(uuid.uuid4()),
                "rules": ["DQ_FMT_001"],
            },
        )
        assert r.status_code == 200
        runs = r.json()["rule_runs"]
        # Only the requested rule should appear
        assert all(rr["rule_id"] == "DQ_FMT_001" for rr in runs)


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
    # All four assignment endpoints + supporting ones should be present
    paths = spec["paths"]
    assert "/harmonise-product" in paths
    assert "/load-data" in paths
    assert "/load-data/{job_id}" in paths
    assert "/compute-dq" in paths
    assert "/detect-anomalies" in paths
    assert "/bad-records" in paths
    assert "/bad-records/{bad_record_id}/resolve" in paths
