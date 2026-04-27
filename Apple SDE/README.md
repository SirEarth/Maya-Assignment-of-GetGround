# Pricing Pipeline — Maya Assignment of GetGround

Multi-partner product-pricing ingestion, harmonisation, data-quality monitoring,
and anomaly detection. Backed by **PostgreSQL 14+** with a real **FastAPI**
service driving the full pipeline end-to-end.

---

## What this project is

A REST API that:

1. **ingests** partner-store product offers from CSV uploads,
2. **standardises** raw product names against an authoritative product registry,
3. **runs 13 Data Quality rules** across three stages (parse → pre-fact gate → post-load soft signals),
4. **stores** every meaningful price event in a Kimball star-schema with bi-temporal price history (Slowly Changing Dimension Type 2),
5. **detects** pricing anomalies with severity + a visualisation payload.

The API service exposes **two call paths** sharing the same 9 internal step helpers:

- **Path A — `POST /pipeline`** — one-click orchestrator: runs the full 9-step pipeline end-to-end with the PRE_FACT hard gate (bad rows do NOT enter `fact_price_offer`).
- **Path B — 4 Task-B sub-modules** — `POST /load-data`, `POST /compute-dq`, `POST /detect-anomalies`, `GET /harmonise-product`. Each independently callable; same 9 steps, but the gate degrades to post-hoc flagging in `dq_bad_records`.

All five endpoints live behind Swagger UI at `/docs`.

---

## Quick start

Tested on macOS 14+. If you already have PostgreSQL 14+, Python 3.9+, and `pip`, jump to **Step 4**.

### Step 1 — Install Homebrew (macOS package manager)

```bash
# Skip if you already have brew. Check with: which brew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### Step 2 — Install Python 3.9+

```bash
brew install python@3.11
python3 --version    # should show 3.11.x or newer
```

### Step 3 — Install PostgreSQL 14+ (Postgres.app)

The simplest path on macOS is [Postgres.app](https://postgresapp.com):

1. Download from <https://postgresapp.com> and drag into `/Applications`.
2. Open the app and click **Initialize** — a 🐘 icon appears in the menu bar.
3. Add `psql` to your `PATH`:
    ```bash
    sudo mkdir -p /etc/paths.d && \
        echo /Applications/Postgres.app/Contents/Versions/latest/bin | \
        sudo tee /etc/paths.d/postgresapp
    ```
4. **Open a new terminal** and verify:
    ```bash
    psql -U $USER postgres -c "SELECT version();"
    ```

(Alternatives: `brew install postgresql@16` or Docker. Adjust `api/db.py` if your username / port differ.)

### Step 4 — Install Python dependencies

```bash
# All commands from here on assume your working directory is this folder
# (the one containing this README, schema.sql, api/, dq/, harmonise/, submission/).
pip3 install fastapi 'pydantic>=2' uvicorn asyncpg psycopg2-binary \
             python-multipart pytest httpx
```

### Step 5 — Build the database and seed reference data

```bash
createdb maya_assignment

psql -d maya_assignment -f schema.sql           # tables, indexes, partitions, dim seeds
psql -d maya_assignment -f dq/rules.sql         # 13 Data Quality check functions
psql -d maya_assignment -f dq/rules_split.sql   # INGEST / PRE_FACT / SEMANTIC orchestrators

python3 seed_bootstrap.py                       # Product Reference + Foreign Exchange rates
```

### Step 6 — Run the test suite

```bash
python3 -m pytest -q
#    → 44 passed in ~1 second (22 harmonise unit + 22 API integration covering
#      Path A pipeline + 4 Path B sub-modules + path-parity tests)
```

### Step 7 — Start the API

```bash
python3 -m uvicorn api.main:app --port 8000

# Browser → http://localhost:8000/docs   (interactive Swagger UI)
```

---

## End-to-end smoke test

### Path A — one-click `POST /pipeline` (9 steps with hard gate)

```bash
curl -X POST http://localhost:8000/pipeline \
     -F "file=@Partner A.csv" -F "partner_code=PARTNER_A"
# → returns aggregated PipelineResponse (rows_loaded, rows_bad,
#   dq_summary, anomalies_total, anomalies_by_severity)
# → fact_price_offer holds only PRE_FACT-passing rows
```

### Path B — 4 Task-B sub-modules in sequence (9 steps with post-hoc flagging)

```bash
# Step 1 — load (parse + harmonise + write fact NO gate + Slowly Changing Dimension Type 2)
LOAD=$(curl -s -X POST http://localhost:8000/load-data \
       -F "file=@Partner B.csv" -F "partner_code=PARTNER_B")
BATCH=$(echo "$LOAD" | python3 -c "import sys,json; print(json.load(sys.stdin)['source_batch_id'])")
echo "batch=$BATCH"

# Step 2 — Validate Data Quality (13 rules → dq_output + dq_bad_records)
curl -X POST http://localhost:8000/compute-dq \
     -H "Content-Type: application/json" \
     -d "{\"source_batch_id\": \"$BATCH\"}"

# Step 3 — Detect anomalies (returns visualization payload)
curl -X POST http://localhost:8000/detect-anomalies \
     -H "Content-Type: application/json" \
     -d "{\"source_batch_id\": \"$BATCH\", \"min_severity\": \"MEDIUM\"}"

# Ad-hoc — Harmonise a single product (algorithm only, no DB lookup)
curl "http://localhost:8000/harmonise-product?q=iP+17+PM+512GB&k=5"

# Bad-records review queue (Task C-2 hook)
curl "http://localhost:8000/bad-records?status=NEW&page_size=10"
```

The two paths cover the same 9 logical steps but differ in gating: Path A blocks PRE_FACT-failing rows from `fact_price_offer`; Path B writes them to fact and flags them in `dq_bad_records` (analytics queries filter via `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL`).

---

## Architecture (one-page view)

```
   Partner CSV  +  partner_code
        │
        ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ FastAPI service                                                 │
  │                                                                 │
  │  Path A — POST /pipeline  (orchestrator, hard gate)             │
  │    1 → 2 → 3 → 4 → 5(gate) → 6 → 7 → 8 → 9                      │
  │              └──── PRE_FACT bad rows blocked here ────┘         │
  │                                                                 │
  │  Path B — Task B sub-modules (each independently callable)      │
  │    POST /load-data       steps 1, 3, 5(no gate), 7, 9           │
  │    POST /compute-dq      steps 2, 4, 6, 9                       │
  │    POST /detect-anomalies step 8                                │
  │    GET  /harmonise-product (algorithm only — also used by step 3)│
  │                                                                 │
  │  Both paths call the SAME 9 step helpers in api/services.py     │
  └─────────────────────────────────────────────────────────────────┘

   9 steps:
     1. Parse CSV          ─► stg_price_offer
     2. INGEST DQ          ─► dq_output + dq_bad_records
     3. Harmonise          ─► back into stg_price_offer
     4. PRE_FACT DQ        ─► dq_output + dq_bad_records
     5. Write fact         ─► fact_price_offer + payment children (Class Table Inheritance)
     6. SEMANTIC DQ        ─► dq_output + dq_bad_records (flag-and-keep)
     7. Slowly Changing Dimension Type 2  ─► fact_partner_price_history
     8. Detect anomalies   ─► response payload (time series + baseline band)
     9. Batch summary      ─► dws_partner_dq_per_batch

   GET /bad-records          ─► business review queue (resolve + replay)
```

**Three-stage DQ rationale.** `INGEST` catches parse/format failures on raw staging. `PRE_FACT` is a HIGH-severity gate that blocks factually wrong rows (country↔currency, partner↔country, harmonise unmatched) from ever reaching `fact_price_offer` **on Path A** — so analytics queries can trust the fact table directly without filter views. On Path B, the gate degrades to post-hoc flagging (bad rows enter fact, analytics filter via `LEFT JOIN dq_bad_records`). `SEMANTIC` runs *after* fact write on single-row soft signals (low-confidence harmonise, category sanity bounds) where business judgment is needed; failing rows stay in fact and are flagged for review. Cross-row pricing patterns (variance, temporal jumps, cross-partner divergence) live in `/detect-anomalies`, not here.

**Why both paths exist.** Task B requires 4 independently callable endpoints — that gives Path B. The orchestrator (`/pipeline`) exists because hard PRE_FACT gating is only possible in a single-process sequential execution: `/load-data` must be independently callable, so it cannot wait for `/compute-dq` to decide whether to write fact. Path A trades flexibility for stronger guarantees; Path B trades guarantees for granular control.

---

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Database | **PostgreSQL 14+** | RANGE partitioning on `crawl_ts_utc`, JSONB for `dq_bad_records.raw_payload` (preserves the original CSV row even when types broke parsing), ENUM for type-safe categoricals (`payment_type_enum`, `harmonise_confidence_enum`, etc.), Materialized View for rolling baselines. |
| API | **FastAPI** + **Pydantic 2** | Auto-generated OpenAPI 3.x schema, request validation at the HTTP boundary, no separate spec to maintain. |
| DB driver | **asyncpg** | Non-blocking; one connection pool shared across all requests. |
| Algorithms | Python (Harmoniser) + PL/pgSQL (DQ rules) | Harmonise runs in-process (Product Reference fits in memory). DQ rules are SQL functions so 13 rules vectorise over millions of rows in one round-trip. |

---

## What's in `submission/`

Five artifacts covering the assignment plus a visual results snapshot:

| File | Maps to |
|---|---|
| `task_a_schema.sql` | **Task A** — clean reconciled schema (Class Table Inheritance for payments, Slowly Changing Dimension Type 2 for history, dq_output + dq_bad_records). |
| `task_b_answers.md` | **Task B** — implementation map for the 4 endpoints with file + line references. |
| `task_c_answers.md` | **Task C** — three short, focused write-ups (data model adaptation, error-handling strategy involving business users, scaling to 1 M records). |
| `api_openapi.json` | Auto-generated OpenAPI 3.x spec; paste into <https://editor.swagger.io> to browse the API contract. |
| `results_showcase.html` | Standalone visual dashboard (Chart.js, no server) — pipeline funnel, harmonise confidence distribution, DQ pass-rates, anomaly visualization, sample bad-records, NZ 154-rows iteration story. Open the file directly in a browser. |

The actual implementation sits alongside this README:

```
.
├── README.md             ← you are here
├── schema.sql            Full schema with design comments
├── seed_bootstrap.py     Loads dim_product_model + dim_currency_rate_snapshot
├── harmonise/            Three-signal Top-K matcher (6 modules + 22 tests)
├── dq/                   13 PL/pgSQL rules + 3-stage orchestrator
├── api/                  FastAPI app + asyncpg pool + 17 integration tests
└── submission/           The 5 artifacts above
```

---

## Author

**huizhongwu** · submitted as the GetGround data engineering exercise.
