# Pricing Pipeline — Project Presentation

> 1-hour project introduction & live demo
> 30-minute Q&A + design / data-model / code review
>
> Prepared by: huizhongwu

**Slide deck flows in 8 sections matching [`demo_guide.md`](demo_guide.md):**

| § | Topic | Slides | Time |
|---|---|---|---:|
| 1 | Project context & what I delivered | 1–3 | 3 min |
| 2 | Live app walkthrough | 4 | 10 min |
| 3 | Technical architecture & pipeline diagrams | 5–6 | 5 min |
| 4 | Task A — Database design (requirements → schema mapping) | 7–9 | 6 min |
| 5 | Task B endpoint design deep-dive | 10–14 | 15 min |
| 6 | Task C — three technical write-ups (C-1 / C-2 / C-3) | 15–17 | 6 min |
| 7 | Reflection — challenges / highlights / next steps | 18–20 | 8 min |
| 8 | Q&A | 21–23 | 30 min |

---

# §1 · Project Context (3 min)

---

## Slide 1 — Title

**Pricing Pipeline: Multi-Partner Product Offer Reconciliation, DQ & Anomaly Detection**

A web-scraping data platform that ingests, harmonises, validates and analyses
product pricing offers from multiple partner stores.

Tech stack: PostgreSQL 14+ • Python 3.9+ • FastAPI • Pydantic 2 • asyncpg

---

## Slide 2 — Problem Statement & Realistic Scale

**The pain points the assignment captures:**

1. **Schema drift** — every partner publishes a different CSV shape
   (Partner A uses monthly instalment fields; Partner B uses a single full price).
2. **Naming chaos** — same product appears under wildly different aliases:
   `iP 17 PM 512GB`, `iP15P 128`, `Apple iPad Air 13-inch (M3) - Starlight 256GB Storage - WiFi`.
3. **Multi-currency, multi-timezone** — AUD vs NZD; AEDT vs NZDT.
4. **Unknown data quality** — no contract guarantees from partners.
5. **Need pricing anomaly alerts** — strategy teams need notification when prices diverge from norm.

**Realistic scale (Apple-sized partner ecosystem):**

| Dimension | Estimate |
|-----------|---------:|
| Authorized retail partners scraped | ~200 |
| Active product models (incl. legacy) | ~1,000 |
| Total SKUs (with colour / storage variants) | ~5,000 – 10,000 |
| Crawls per partner per day | 4 – 12 |
| **Raw observations per day** | **~1.6 million** |
| Real price-change events (~5–10%) | ~80k – 160k / day |
| Change events per year | ~30 – 60 million |

**Designed for "medium-large analytical workload":** PostgreSQL + monthly partitions + targeted indexes — not streaming, not sharded.

---

## Slide 3 — What I Delivered

**Task A — Database (✅):** 29 tables, full Kimball star schema
- **Class Table Inheritance** for payment polymorphism (no sparse columns)
- **Slowly Changing Dimension Type 2** (`fact_partner_price_history`) for compressed price history
- **3 DQ tables** (`dq_rule_catalog` / `dq_output` / `dq_bad_records`) with workflow state

**Task B — 4 endpoints + orchestrator (✅ all implemented, real PostgreSQL backend):**

The API is **one independent service** with two call paths:

- **Path A — `POST /pipeline`** (orchestrator) — runs all 9 steps in a single PostgreSQL transaction; PRE_FACT hard gate keeps bad rows out of `fact_price_offer`.
- **Path B — 4 Task-B sub-modules** (independently callable):
  - `POST /load-data` — parse + harmonise + write fact (no gate) + Slowly Changing Dimension Type 2
  - `POST /compute-dq` — 13 PL/pgSQL rules across 3 stages → `dq_output` + `dq_bad_records`
  - `POST /detect-anomalies` — 4-signal detection (STATISTICAL / TEMPORAL / CROSS_PARTNER / SKU_VARIANCE) + visualization payload
  - `GET /harmonise-product` — Top-K with score breakdown

Both paths invoke the **same 9 internal step helpers** in `api/services.py` — zero code duplication.

**Task C — 3 short write-ups (✅ in `submission/task_c_answers.md`):**
- C-1 data model adaptation when new partners onboard
- C-2 DQ + business correction loop
- C-3 scaling to 1 M records

**Quality bar:** **47 / 47 automated tests passing** (22 harmonise unit + 25 API integration covering Path A pipeline + 4 Path B sub-modules + path-parity tests).

**Architectural innovation beyond the spec:** **3-stage DQ with severity-driven policy** (INGEST → PRE_FACT gate → SEMANTIC) — combined with the Path A orchestrator, makes `fact_price_offer` trustworthy by construction.

---

# §2 · Live App Walkthrough (10 min)

---

## Slide 4 — Live Demo Plan (Swagger UI + VSCode)

**Goal:** show the full pipeline end-to-end on real PostgreSQL — entirely in the browser via Swagger UI, plus VSCode for SQL inspection. **No terminal / curl needed.**

| Step | Where | What |
|---|---|---|
| 1 | Browser (Swagger UI) | Scroll through all 9 endpoints; explain dual-path (Path A `/pipeline` + Path B 4 Task-B sub-modules). Pydantic auto-generated. |
| 2 | Swagger UI → **`POST /pipeline`** | Path A demo: "Try it out" → upload `Partner A.csv` + `partner_code=PARTNER_A` → Execute. Single response includes `dq_summary` + `anomalies_total`. Copy `source_batch_id`. |
| 3 | Swagger UI → **`POST /load-data`** then **`/compute-dq`** then **`/detect-anomalies`** | Path B demo: same 9 steps in three calls. Tell **NZ story** during `/load-data` upload of `Partner B.csv` (154 rows; resolver fix; replay loop). Use the `/compute-dq` response to show post-hoc flagging — bad rows ARE in fact, just labelled. |
| 4 | Browser → `results_showcase.html` | Open the static visual dashboard; walk through the cards: headline numbers → pipeline funnel (with the 9 Apple Watch SKUs blocked story) → harmonise confidence pie → DQ pass-rate bars → anomaly time-series with band → sample harmonise breakdowns → sample bad records → NZ 154-rows iteration story |
| 5 | (Optional) VSCode Database Panel | One quick `demo_queries.sql §0` sanity check to prove the dashboard reflects live DB, not a pre-rendered mock. Other SQL sections kept as drill-down for Q&A follow-ups. |

**Expected baseline numbers after both uploads:**

| Table | Rows |
|---|---:|
| `stg_price_offer` | ~4 208 |
| `fact_price_offer` | ~4 174 |
| `fact_payment_full_price` / `fact_payment_instalment` | 1 066 / 3 108 |
| `fact_partner_price_history` | ~120 |
| `dq_bad_records` | ~77 |
| `dq_output` | 26 (= 2 batches × 13 rules) |

**Punchline to land at end:** *"Notice the funnel — every stg row preserved with `raw_payload`, but only PRE_FACT-passing rows enter `fact_price_offer`. Anything HIGH severity is blocked here. So `SELECT * FROM fact_price_offer` is safe to query directly — analytics never need a filter view."*

---

# §3 · Technical Architecture & Pipeline Diagrams (5 min)

---

## Slide 5 — Architecture (3-Layer · Dual Call Path)

```
┌────────────────────────────────────────────────────────────────────┐
│                         Client / Interviewer                       │
│            Swagger UI · curl · future Frontend (React)             │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ HTTP/JSON
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  API Layer · FastAPI + Pydantic 2 (api/main.py)                    │
│                                                                    │
│   ▸ Path A (orchestrator):  POST /pipeline                         │
│   ▸ Path B (Task-B sub-modules):                                   │
│        POST /load-data    POST /compute-dq                         │
│        POST /detect-anomalies   GET /harmonise-product             │
│   ▸ Support: /bad-records, /load-data/{id}, /health                │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ both paths invoke the SAME helpers ↓
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  Service Layer · Async Python (api/services.py)                    │
│                                                                    │
│  9 step helpers (shared by Path A and Path B):                     │
│   1. parse_csv_to_stg            6. run_semantic_dq                │
│   2. run_ingest_dq               7. update_scd2                    │
│   3. harmonise_in_stg            8. detect_anomalies_for_batch     │
│   4. run_prefact_dq              9. write_batch_summary            │
│   5. write_stg_to_fact(gate=…)                                     │
│                                                                    │
│  Service entry points: run_pipeline · submit_load_job ·            │
│                         compute_dq_service · detect_anomalies_…    │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ asyncpg pool
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  Data Layer · PostgreSQL 14+                                       │
│                                                                    │
│  ▸ Dimensions (14): dim_country / dim_partner / dim_product_*      │
│                     dim_currency_rate_snapshot · dim_anomaly_*     │
│  ▸ Facts (5):       fact_price_offer (Class Table Inheritance)     │
│                     fact_payment_full_price · fact_payment_*       │
│                     fact_partner_price_history (Slowly Changing    │
│                                                  Dimension Type 2) │
│                     fact_anomaly                                   │
│  ▸ DQ (3):          dq_rule_catalog (13 rules) · dq_output         │
│                     dq_bad_records (workflow + raw_payload JSONB)  │
│  ▸ DWS:             materialized view + summary tables             │
└────────────────────────────────────────────────────────────────────┘
```

**Why this split:**
- **API layer** offers two paths over the same logic. Path A (`/pipeline`) is the one-click orchestrator with strict gating. Path B (4 Task-B sub-modules) provides independently callable endpoints — what the assignment literally specifies. Pydantic auto-validates and auto-generates OpenAPI.
- **Service layer** holds the **9 shared step helpers** plus the entry points that orchestrate them. Both paths call the same helper functions — zero code duplication; the only difference is orchestration order and the `gate` parameter to `write_stg_to_fact`.
- **Data layer** is PostgreSQL with a 29-table star schema; asyncpg pool gives concurrent queries without thread overhead.

---

## Slide 6 — Business Pipeline (9 Steps · Two Call Paths)

```
   Partner CSV upload + partner_code
           │
           ▼
   ① Parse → stg_price_offer (raw_payload JSONB preserved)
           │
           ▼
   ② INGEST DQ (8 rules) — null / format / range / conditional
           │  passing rows → dq_status='INGEST_PASSED'
           ▼
   ③ Harmonise — Top-K 3-signal match, write product_model_id back to stg
           │
           ▼
   ④ PRE_FACT DQ (3 rules) — country↔currency / partner↔country /
           │  harmonise unmatched. Failing rows → dq_bad_records.
           │  Passing rows → dq_status='PRE_FACT_PASSED'
           ▼
   ⑤ write_stg_to_fact(gate=…)   ⚠️ KEY DIVERGENCE POINT:
           │
           │  Path A  (gate=True)   only PRE_FACT_PASSED rows enter
           │     → fact clean        fact_price_offer (Class Table
           │                         Inheritance: full_price /
           │                         instalment 1:1 children)
           │
           │  Path B  (gate=False)  all parseable rows enter fact
           │     → fact has flagged  later analytics filter via
           │       rows; review via   LEFT JOIN dq_bad_records
           │       /bad-records       WHERE bad_record_id IS NULL
           ▼
   ⑥ SEMANTIC DQ (2 rules) — single-row soft signals (low-confidence
           │  harmonise, category sanity bounds). Flag-and-keep policy.
           ▼
   ⑦ Slowly Changing Dimension Type 2 — single CTE:
           │   latest → existing → changed → closed → insert
           │  → fact_partner_price_history
           ▼
   ⑧ detect_anomalies_for_batch — 4 signals (STATISTICAL / TEMPORAL /
                                    CROSS_PARTNER / SKU_VARIANCE) → fact_anomaly
           │  visualization payload (series + band + cross-partner JSON)
           ▼
   ⑨ write_batch_summary → dws_partner_dq_per_batch (UPSERT, idempotent)

   ════════════ Two paths, same 9 helpers ══════════════════════════════

   Path A — POST /pipeline           single transaction; interleaved
                                      1→2→3→4→5(gate)→6→7→8→9
                                      `fact_price_offer` is clean by gate

   Path B — sub-module sequence      multi-transaction; grouped
     POST /load-data                 helpers 1, 3, 5(no gate), 7
     POST /compute-dq                helpers 2, 4, 6
     POST /detect-anomalies          helper 8
     (each endpoint UPSERTs step 9 → summary fills incrementally)
                                      `fact` contains flagged rows

   Future extension: anomaly alerting (out of demo scope) —
   fact_anomaly → dim_alert_policy routing → Slack HIGH / Email MED / UI LOW
```

**Key design call:** the 9 steps run as helper functions in `api/services.py`. **Path A** invokes them in interleaved order inside one PostgreSQL transaction — any failure rolls back the whole batch; the PRE_FACT gate at step 5 keeps bad rows out of fact. **Path B** invokes them in grouped order across three transactions — Task B's literal contract of 4 independently callable endpoints; the gate degrades to post-hoc flagging in `dq_bad_records`. Both paths cover all 9 logical steps — same code, different orchestration.

**Three DQ stages, three policies:**

| Stage | Where | Policy | Why |
|---|---|---|---|
| INGEST | raw stg | Block at staging | Parse / format errors don't deserve fact |
| **PRE_FACT** | enriched stg | **Block from fact** | Factual errors pollute analytics |
| SEMANTIC | fact | Flag-and-keep | Soft signals need business judgment |

---

# §4 · Task A — Database Design (6 min)

---

## Slide 7 — Task A: Requirements → Design Mapping

The assignment specifies **five requirements** for Task A. Each maps to a deliberate design choice:

| # | Requirement | Design response | Where it lives |
|---|---|---|---|
| 1 | Reconcile differences between partner data sources | Single normalised fact table fed by a harmonise pipeline that maps raw partner names to canonical models | `fact_price_offer` + `dim_product_model` |
| 2 | Multiple payment methods **WITHOUT a sparse table design** | **Class Table Inheritance (CTI)** — parent fact + 1:1 child tables per payment type | `fact_price_offer` + `fact_payment_full_price` / `fact_payment_instalment` |
| 3 | Standardised product identifiers | Two-tier keying: SERIAL surrogate for joins + VARCHAR natural key for idempotency | `dim_product_model.product_model_id` + `model_key` |
| 4 | Track temporal data | Bi-temporal facts (business + system time) + **Slowly Changing Dimension Type 2** history table | `fact_price_offer.{crawl_ts_utc, ingested_at}` + `fact_partner_price_history` |
| 5 | dq_output and bad_records tables | Three DQ tables: rule registry + per-(rule, run) summary + per-failing-record audit | `dq_rule_catalog` + `dq_output` + `dq_bad_records` |

**By the numbers:**

| Tables | 29 | 14 dimensions · 5 facts · 3 DQ · 4 DWS · 3 partition defaults |
|---|---:|---|
| ENUM types | 4 | `payment_type_enum` · `harmonise_confidence_enum` · `bad_record_status_enum` · `product_lifecycle_enum` |
| Partitioning | RANGE by month | on `crawl_ts_utc` for `fact_price_offer` and its CTI children |
| Aggregation | A+MV hybrid | materialised view feeding the baseline summary table |

**Why these particular choices and not the obvious alternatives?**

| Alternative | Why we rejected it |
|---|---|
| Sparse columns for payment | Wastes storage, prevents NOT NULL / CHECK constraints on payment-specific fields, explicitly forbidden by the brief |
| JSONB for payment payload | Database can't enforce schema on `payment_details` JSONB; loses SQL type safety; index path queries are awkward |
| SCD-1 (overwrite-in-place) | Loses history — anomaly detection's 30-day rolling baselines would have nothing to read |
| One unified DQ table (no catalog / output split) | Aggregate dashboards (per-rule pass rate trend) become slow joins; per-record audit explodes the table; we want both grains |

The next two slides expand the two highest-leverage choices: **Star Schema** (Slide 8) and **SCD-2 + Aggregate Hybrid** (Slide 9). The full clean schema for submission lives in `submission/task_a_schema.sql` (758 lines).

---

## Slide 8 — Data Model: Star Schema (Kimball)

```
              ┌──────────────────┐
              │ dim_product_     │
              │ category         │
              └─────────┬────────┘
                        │
   ┌────────────┐   ┌───┴────────────┐    ┌────────────┐
   │ dim_       │   │ dim_product_   │    │ dim_       │
   │ partner    │   │ model          │    │ country    │
   └─────┬──────┘   └────────┬───────┘    └─────┬──────┘
         │                   │                   │
         │            ┌──────┴──────┐            │
         │            │ dim_product │            │
         │            │ _sku        │            │
         │            └──────┬──────┘            │
         │                   │                   │
         └───────┬───────────┼───────────────────┘
                 │           │
                 ▼           ▼                    ┌────────────┐
            ┌────────────────────┐                │ dim_       │
            │ fact_price_offer   │ ◄──────────────┤ currency   │
            │ (parent, CTI)      │                └────────────┘
            └────────┬───────────┘                ┌─────────────┐
                     │                            │ dim_        │
         1:1 ────────┼─────────                   │ currency_   │
         ▼                    ▼                   │ rate_snap.. │
   fact_payment_         fact_payment_            └─────────────┘
   full_price            instalment
```

**Five key design choices:**
1. **Class Table Inheritance** for payment polymorphism → no sparse columns (assignment requirement).
2. **Bi-temporal facts** — `crawl_ts_utc` (business time) + `ingested_at` (system time).
3. **Currency frozen at load time** — `effective_total_local` + USD + FX rate + FX date on the fact row → audit trail without runtime JOINs.
4. **Range-partitioned by month** — fast pruning, easy archival via DETACH PARTITION.
5. **Generated stored column** `crawl_date_local` → market-local date for regional dashboards without runtime conversion.

---

## Slide 9 — Compressed Price History + Aggregation (SCD-2 + A+MV)

**Slowly Changing Dimension Type 2** — `fact_partner_price_history` only inserts a new row when the price actually changes:

```
product=42, partner=A, country=AU, payment=FULL
  history_id=1  price=1999  valid_from=2025-10-01  valid_to=2025-11-15
  history_id=2  price=1899  valid_from=2025-11-16  valid_to=2025-12-24  ← changed
  history_id=3  price=1799  valid_from=2025-12-25  valid_to=NULL        ← current
```

- **20–50× compression** vs raw event stream
- **As-of queries:** `WHERE valid_from_date <= X AND COALESCE(valid_to_date, '9999-12-31') >= X`

**A+MV (Aggregate table + Materialized View) hybrid for baselines:**

```
fact_partner_price_history
        │ MATERIALIZED VIEW (calculation in SQL)
        ▼
mv_baseline_staging         (today's rolling stats: 7d / 30d / 90d windows)
        │ INSERT ... ON CONFLICT (write-time dedup)
        ▼
dws_product_price_baseline_1d
        │ millisecond lookup
        ▼
POST /detect-anomalies
```

**Why hybrid:** MV keeps logic in plain SQL (reviewable, testable); physical table preserves history (yesterday's baseline isn't overwritten); write-time dedup avoids storing identical rows day after day.

---

# §5 · Task B Endpoint Design (15 min)

---

## Slide 10 — API Surface

| Endpoint | Method | Path | Purpose | Implementation |
|----------|--------|---|---------|----------------|
| `/pipeline` | POST | **A — orchestrator** | One-click 9-step pipeline; PRE_FACT hard gate | `api/services.py:run_pipeline` |
| `/load-data` | POST | B — sub-module #1 | Parse + harmonise + write fact (no gate) + Slowly Changing Dimension Type 2 | `api/services.py:submit_load_job` |
| `/load-data/{job_id}` | GET | B — support | Poll progress | `api/services.py:get_job_status` |
| `/compute-dq` | POST | B — sub-module #2 | Run all 13 DQ rules → 2 tables | `api/services.py:compute_dq_service` + `dq/rules.sql` |
| `/detect-anomalies` | POST | B — sub-module #3 | 4-signal anomaly detection (STATISTICAL/TEMPORAL/CROSS_PARTNER/SKU_VARIANCE) + visualization payload | `api/services.py:detect_anomalies_service` |
| `/harmonise-product` | GET | B — sub-module #4 | Top-K canonical match + score | `harmonise/` (6 modules) |
| `/bad-records` | GET | Support | List flagged records | `api/services.py:list_bad_records` |
| `/bad-records/{id}/resolve` | POST | Support | Resolve + optionally replay | `api/services.py:resolve_bad_record` |
| `/health` | GET | Support | Liveness probe | `api/main.py:health` |

**Built with FastAPI + Pydantic 2:** typed contracts everywhere; auto-generated OpenAPI 3.x at `/docs` (Swagger UI) and `/redoc`.

**Tested:** 47 automated tests (22 harmonise unit + 25 API integration covering Path A pipeline + 4 Path B sub-modules + path-parity tests).

---

## Slide 11 — `GET /harmonise-product`

**Problem:** Map raw partner names to canonical product models.
- `iP 17 PM 512GB` → `iPhone 17 Pro Max 512GB` (HIGH 0.946)
- `iP15P 128` → `iPhone 15 Pro 128GB` (HIGH 0.845, no "GB" suffix)
- `Apple iPad Air 13-inch (M3) - Starlight 256GB Storage - WiFi` → same canonical model

**Three-signal hybrid scorer:**
```
score = 0.5 × attribute_match    (structured: category, storage, chip, model)
      + 0.3 × token_jaccard       (cleaned token set overlap)
      + 0.2 × char_fuzz_ratio     (character-level fuzzy backup)
```

**Structural override:** `attribute_match >= 0.95` → force HIGH confidence regardless of combined score. Avoids manual-review overload from verbose-but-correct partner names.

**Three-layer abbreviation dictionary:**
1. Manual core (~30 entries) — domain knowledge, can't be auto-mined
2. Data-driven mining (offline) — TF-IDF over Product Ref descriptions [future]
3. Business loop — low-confidence matches → `dq_bad_records` → reviewer confirms → tokens promoted

**Why no embeddings:** 281 reference rows; structured matching is enough; explainability matters more for DQ review than vector similarity.

---

## Slide 12 — `POST /pipeline` + `POST /load-data` (Path A vs Path B)

**Two endpoints, same 9 helpers, different orchestration** (see Slide 6 diagram).

**`POST /pipeline` — orchestrator**, single PostgreSQL transaction
- Runs all 9 helpers in interleaved order: 1→2→3→4→5(gate=True)→6→7→8→9
- **PRE_FACT hard gate** at step 5: bad rows do NOT enter `fact_price_offer`
- Returns aggregated `PipelineResponse` (rows_loaded + dq_summary + anomalies_total)
- Use when: production one-click ingest with strong cleanliness guarantees

**`POST /load-data` — Task-B literal sub-module**, independently callable
- Covers helpers 1 (parse), 3 (harmonise), 5 with gate=False, 7 (Slowly Changing Dimension Type 2)
- **No gate**: all parseable rows enter fact; `/compute-dq` flags bad rows post-hoc
- Returns HTTP 202 + `job_id` — matches the C-3 async contract
- Use when: debugging, ad-hoc loads, scenarios where DQ runs separately

**Five design calls hold for both paths:**

**1. Same code path.** Both endpoints invoke the same 9 helpers — only orchestration order and the `gate` flag differ. Zero duplication.

**2. Three-stage DQ with severity policy.** INGEST blocks at stg; PRE_FACT either blocks at fact (Path A) or flags post-hoc (Path B); SEMANTIC always flags-and-keeps after fact write.

**3. Class Table Inheritance for payment.** `fact_price_offer` has a `payment_type_enum` discriminator; `fact_payment_full_price` and `fact_payment_instalment` are 1:1 child tables with their own NOT NULL + CHECK constraints (impossible in a sparse single-table design).

**4. Currency frozen on fact row.** Helper 5 JOINs `dim_currency_rate_snapshot` once at load time and stores `effective_total_local`, `effective_total_usd`, `fx_rate_to_usd`, `fx_rate_date` on the fact. Future FX corrections don't retroactively change historical USD values.

**5. Slowly Changing Dimension Type 2 reconciliation in one CTE.** Helper 7 uses `latest → existing → changed → closed → insert` in a single SQL statement. No row-by-row Python.

**Why both endpoints exist.** Hard PRE_FACT gating is **only possible in single-process sequential execution** — `/load-data` must be independently callable per Task B, so it cannot wait for `/compute-dq` to decide whether to write fact. `/pipeline` exists to recover the gating semantic by running both inside one orchestrator. The trade-off is documented; both are honest contracts.

---

## Slide 13 — `POST /compute-dq` (DQ 3-Stage Strategy)

**13 rules, three stages — each stage with a different policy:**

| Stage | Where it runs | Rule count | Policy on failure | Examples |
|-------|---------------|-----------:|-------------------|----------|
| **INGEST** | raw `stg_price_offer` | 8 | Row stays in stg, never reaches `fact_price_offer` | Required fields, format, range, payment-type conditional, country/currency code resolution |
| **PRE_FACT** | enriched `stg_price_offer` | 3 | **HIGH-severity gate** — row blocked from fact | Country↔currency, partner↔country, harmonise unmatched |
| **SEMANTIC** | `fact_price_offer` | 2 | Row stays in fact, flagged | Harmonise low confidence, category sanity bounds |

**Why a PRE_FACT gate.** Without it, factual errors land in `fact_price_offer` and downstream analytics need filter views to exclude unresolved HIGH severity records. With the gate, `SELECT * FROM fact_price_offer` is safe to query directly.

**Why SEMANTIC stays small + soft.** Only single-row data-quality concerns that need business judgment live here — low confidence might be a new product (add to dictionary), category-band violations might be real promotions (don't auto-discard). Cross-row pricing patterns (variance, duplicates, temporal jumps) belong in `/detect-anomalies`, not in DQ.

**Implementation:** every rule is a PL/pgSQL function returning `(row_ref, failed_field, error_message, raw_payload)`. **One DB call replaces 13 M Python checks** at 1 M-row scale.

**Business-user closure (Task C-2):**
```
NEW  →  IN_REVIEW  →  RESOLVED (replay batch) | IGNORED
```

**Real example — the NZ 154-row story:** initial `DQ_FMT_001` only accepted `'New Zealand'`; 154 rows with `'NZ'` failed Ingest DQ; resolver fix → replay → rows promoted to fact. **Real DQ → rule iteration → replay loop.**

---

## Slide 14 — `POST /detect-anomalies`

**Four anomaly types — all implemented, each detected independently:**

| Type | Question answered | Compares against | Status |
|------|-------------------|---|:---:|
| STATISTICAL | "Is this price outside historical norms?" | 30-day rolling baseline | ✅ |
| TEMPORAL | "Did the price suddenly change?" | last valid price (same partner) | ✅ |
| CROSS_PARTNER | "Is this price way off from peers?" | other-partner median (`v_partner_price_current`) | ✅ |
| SKU_VARIANCE | "Is the spread within one batch suspicious?" | same-model same-day observations (z-score) | ✅ |

**Behaviour on sample data (`Partner A.csv` + `Partner B.csv`, one upload each):**

| Signal | Triggers? | Why / when it WOULD trigger naturally |
|---|:---:|---|
| STATISTICAL | ❌ 0 | Needs ≥2 baseline samples in last 30 days; first ingest has only 1 SCD-2 row per (product, country). Production: daily crawls accumulate 8–30 rows per 30 days → fires on any ≥10% deviation. |
| TEMPORAL | ❌ 0 | Needs a *prior* SCD-2 row for the same key. Verified in dev with a synthetic INSTALMENT spike (`iP 17 PM 512GB` $1,689 → $4,680) → fired **HIGH** with `signal_score = 1.000`. |
| CROSS_PARTNER | ❌ 0 | Sample data has Partner A only in AU + Partner B only in NZ → no `(product, country)` peer overlap. Verified in dev by SQL-injecting one Partner A row in NZ → fired **20 HIGH** on next Partner B upload. |
| SKU_VARIANCE | ✅ **38** | Self-contained, fires on first ingest. Catches same-model same-day outliers; in our data picks up 1 MEDIUM + 37 LOW from intra-partner per-color price variance. |

**Reading this honestly.** All four detectors work — the 0-counts on sample data reflect *data shape* (single-country-per-partner, single ingest, no price changes), not algorithmic gaps. Production data (multi-partner overlap + continuous crawling) lets all four fire naturally.

**Per-signal severity (NOT a single combined score).** Each triggered signal produces its own row in `fact_anomaly`. If three signals fire on one offer, three rows are created — each routes to the right team via `dim_alert_policy`.

**Contextual adjustments stored per row:**
- `lifecycle_factor` — NEW / STABLE / LEGACY / EOL moderates threshold
- `event_suppression_factor` — `dim_market_event` (Apple launch / Black Friday) suppresses known volatility
- `category_sensitivity` — wider tolerance for high-volatility categories (AirPods)

**All thresholds in `dim_anomaly_threshold`** — config-driven, no magic numbers; Logistic Regression calibration plan documented.

**Visualization payload (decoupled from rendering):**
```json
{
  "type": "time_series_with_band",
  "series": [{date, price_usd, is_anomaly}, ...],
  "baseline_band": {"mean": ..., "lower": ..., "upper": ...},
  "cross_partner_comparison": {"PARTNER_A": ..., "PARTNER_B": ...}
}
```
Same payload feeds Chart.js, Slack cards, PDF reports — frontend does the drawing.

---

# §6 · Task C — Three Technical Write-ups (6 min)

The brief asks three specific questions in Task C. Concise answers below; full version in [`submission/task_c_answers.md`](../Apple_SDE_Submission/submission/task_c_answers.md).

---

## Slide 15 — Task C-1: Adapting to New Partners

**Q.** *How does the data model adjust when new partner store data is inducted?*

The Kimball star schema isolates partner change to a **single dimension row**. Fact tables stay stable for years.

| Change | What needs to happen | Schema impact |
|--------|----------------------|---------------|
| **New partner, existing payment scheme** | `INSERT INTO dim_partner` (one row) | **None.** Existing `fact_price_offer` rows route to the new `partner_id` automatically. |
| **New payment type** (e.g. Buy Now Pay Later) | `ALTER TYPE payment_type_enum ADD VALUE 'BNPL'` + `CREATE TABLE fact_payment_bnpl` (CTI child) | One new child table; **existing rows untouched.** Class Table Inheritance is the payoff. |
| **New country** | `INSERT INTO dim_country` (+ `dim_timezone` if a new IANA zone) | **None on facts.** |
| **Unknown product category in partner feed** | Detected by `DQ_HARM_002` (PRE_FACT gate) → `dq_bad_records` → business review. If legitimate, Apple catalog team adds to Product Reference, then `INSERT INTO dim_product_category`, replay batch. **Never auto-discovered.** | **None on facts.** Bad rows never reach `fact_price_offer`. |

**The takeaway.** Dimensions are designed to absorb business change; fact schemas stay stable for years. New partners onboard via configuration, not migrations — that is the star-schema's primary payoff.

---

## Slide 16 — Task C-2: Error Handling + Data Quality Strategy

**Q.** *Design an error handling and data quality strategy that may involve business users to perform correction action.*

Three-tier closure loop. Detection is automated, triage is business-driven, and the system **learns** from each correction.

**Tier 1 — Automated detection (zero human in the loop)**
- Every `POST /load-data` runs **13 DQ rules** in three stages:
  - **INGEST** (8 rules) — null / format / range / conditional checks on raw staging
  - **PRE_FACT** (3 HIGH-severity rules) — country↔currency, partner↔country, harmonise unmatched. **Failing rows blocked from fact_price_offer.**
  - **SEMANTIC** (2 rules) — low-confidence harmonise + category sanity. Flag-and-keep on fact.
- Failing records → `dq_bad_records` with `raw_payload` JSONB preserving the **full original CSV row**.

**Tier 2 — Business-user triage (visual review interface)**
- `GET /bad-records?status=NEW` lists open items with raw payload + failed rule + severity
- Reviewer picks an action via `POST /bad-records/{id}/resolve`:
  - **`RESOLVED + replay_batch=true`** — fix the dictionary or rule, surgically re-ingest just that `source_batch_id` (not full reload)
  - **`IGNORED`** — close the ticket without changing data
- Workflow state on the row (`status`, `assignee`, `resolved_at`, `resolution_notes`) — full audit trail.

**Tier 3 — Learning loop**
- Resolved records feed back: harmonise dictionary additions (Layer 3), threshold tuning in `dim_anomaly_threshold`, new DQ rules when patterns emerge.

**Real example — the 154-row NZ story.** Partner B shipped 154 rows with `COUNTRY_VAL = "NZ"` (ISO code, not full name `"New Zealand"`). `DQ_FMT_001` flagged them. Reviewer saw both forms are legitimate; resolver was hardcoded to full names. Fix: extend `_COUNTRY_NAME_MAP` to accept ISO codes; replay; **all 154 rows promoted to fact_price_offer cleanly.** **DQ → rule iteration → replay loop in action.**

---

## Slide 17 — Task C-3: Scaling to 1 M Records

**Current sync flow:** 3–7 hours for 1 M rows; HTTP times out long before completion.

**Redesign — 5 changes (3 already implemented, 2 are the production gap):**

| # | Change | Status | Speedup contribution |
|---|---|:---:|---|
| 1 | Async pipeline: HTTP 202 + S3 + `ingest_job` + chunked workers | Designed | enables horizontal scaling |
| 2 | PostgreSQL `COPY` instead of `INSERT` | Designed | **50–100× write speedup** |
| 3 | DQ rules executed in SQL (one DB call replaces 13 M Python checks) | ✅ Implemented | done |
| 4 | Reference data cached in-memory (zero per-row DB lookups) | ✅ Partial | done (Harmoniser + country/currency dicts) |
| 5 | Bulk Slowly Changing Dimension Type 2 update via single CTE | ✅ Implemented | done |

**Performance targets:**
- 1 M rows: 3–7 hours → **~90 seconds** (~200×)
- 10 M rows: feasible in ~15 minutes with partition-aware sharding

**Migration path is incremental:**
1. Swap COPY in place of `executemany` (1 day, immediate 50× ingest speedup)
2. Build async pipeline (`ingest_job` table + queue + worker pool) — bigger lift, unlocks both async UX and horizontal scaling

See [`task_c_answers.md`](../Apple_SDE_Submission/submission/task_c_answers.md) C.3 for full details.

---

# §7 · Reflection (8 min)

---

## Slide 18 — Challenges & Iterations

**Things that turned out harder than expected:**

1. **Schema iteration.** First draft used a wide table with sparse payment columns; assignment explicitly forbids sparse — refactored to **Class Table Inheritance**. DQ started as 2 stages (INGEST + SEMANTIC); during integration testing realised "fact has invalid country/currency rows" → added **PRE_FACT gate**.

2. **Harmonise edge cases.** Partner A ships `iP15P 128` (no "GB" suffix); naive token-match gives MEDIUM/LOW. Fixed with two heuristics:
   - **Storage-set fallback** — standalone digits matching `{64, 128, 256, 512, 1024, 2048}` get treated as GB
   - **Structural override** — if attribute_match alone ≥ 0.95, force HIGH bucket (avoids manual-review overload)

3. **Real-world dirt the spec hides.** Discovered Partner B has 154 rows where `COUNTRY_VAL = "NZ"` (ISO code, not full name). DQ engine flagged them; root cause was the resolver only accepting full names. **Real DQ → rule iteration → replay loop.**

4. **SCD-2 same-day boundary.** Re-running tests hit `valid_from > valid_to` constraint failures when an observation arrived on the same day as an existing history row's valid_from. Fixed by adding a same-day guard in the change-detection CTE.

5. **Threshold calibration is the silent killer.** First draft had hand-picked weights (`0.4 / 0.3 / 0.3`). Fixed by introducing `dim_anomaly_threshold` with documented calibration plans (Logistic Regression for weights; percentiles for thresholds; A/B testing for suppression factors).

6. **Removing things is design too.** Dropped 3 aggregate tables that didn't earn their keep (`dws_price_offer_market_local_1d`, `dws_price_offer_td`, `dws_cross_partner_comparison_1d`) once I confirmed no API endpoint needed them.

---

## Slide 19 — Design Highlights I'm Proud Of

**1. Orchestrator + sub-modules with shared 9 helpers.** Task B requires 4 independently callable endpoints — that's Path B. But hard PRE_FACT gating fundamentally needs single-process sequential execution, so I added `POST /pipeline` as the orchestrator (Path A). Both paths invoke the **same 9 helper functions** in `api/services.py` — zero duplication; the only difference is orchestration order and the `gate` flag passed to `write_stg_to_fact`. Honest trade-off: Path A trades flexibility for stronger guarantees; Path B trades guarantees for granular control.

**2. Three-stage DQ with severity-driven policy.** The architectural call I'd defend in any review:
   - INGEST stops at staging (parse errors)
   - PRE_FACT blocks factual errors from entering fact (Path A) or flags post-hoc (Path B)
   - SEMANTIC flags soft signals after fact write

   **Result:** on Path A, `fact_price_offer` is trustworthy by construction — downstream analytics queries don't need filter views. On Path B, analytics use `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL` to filter; demonstrates why the orchestrator is worth the extra endpoint.

**3. DQ rules are PL/pgSQL functions.** 13 rules executed in PostgreSQL, not Python. **One DB call replaces 13 M Python checks** at 1 M rows. Plus: `dq_rule_catalog` is metadata-driven — adding a rule = one INSERT, no code change.

**4. Explainable harmonise.** Three signals + structured override → every match has a transparent breakdown. Business reviewers can see *why* the matcher decided something, which is invaluable for triage. Vector embeddings would have been a black box.

**5. Visualization payload decoupled from rendering.** `/detect-anomalies` returns structured JSON, not images. Same payload feeds Chart.js dashboards, Slack alert cards, PDF reports — three consumers from one definition.

**6. `fact_anomaly` one-row-per-signal (not per-offer).** An offer that trips multiple signals appears as multiple rows, each routable to a different team. Combining them into a composite would dilute or hide individual concerns.

**7. Adapting to a new partner is a configuration change, not a migration.** Adding Partner C = `INSERT INTO dim_partner`; new payment type = ALTER TYPE + new CTI child; new country = `INSERT INTO dim_country`. Star-schema decoupling pays off (full table on Slide 15 / Task C-1).

---

## Slide 20 — What I'd Build Differently

**1. Anomaly signal weight calibration.** All 4 signals (STATISTICAL / TEMPORAL / CROSS_PARTNER / SKU_VARIANCE) are implemented with hand-set tier thresholds. Production should learn per-signal severity weights from confirmed-positive feedback in `dq_bad_records` via Logistic Regression — turns the threshold ladder into a continuously tuned model.

**2. Async pipeline + COPY.** C-3 is the production gap. For the take-home demo, sample data finishes in seconds; for 1 M rows, swapping in `COPY` (1 day) plus adding `ingest_job` table + worker pool would deliver the ~90 sec target.

**3. Harmonise Layer 2 — data-driven mining.** Currently only Layer 1 (manual dictionary) and Layer 3 (business loop). Layer 2 (TF-IDF + N-gram co-occurrence over Product Ref Long/Short Description alignment) is scaffolded but not implemented.

**4. Sentence-transformer fallback.** Pluggable via `score_fn="embedding"` — not enabled because at 281 reference rows it adds dependency without measurable benefit. Worth revisiting if Reference grows 10×.

**5. Real-time alerting via Kafka / Webhooks.** Current design has Postgres `LISTEN/NOTIFY` trigger as a placeholder pattern; production would be Kafka producer or webhook fan-out via `dim_alert_channel`.

**6. Observability — `ingest_job` + Prometheus metrics.** Currently relies on `dws_partner_dq_per_batch` for batch-level KPIs. Production would add per-job duration / chunk progress / retry counts to a dedicated metrics endpoint.

---

# §8 · Q&A (30 min)

---

## Slide 21 — Q&A Cheat Sheet

**Likely deep-dive questions and where the answer lives:**

| Question | Where to look |
|----------|---------------|
| Why an orchestrator (`/pipeline`) AND 4 sub-modules? Isn't that redundant? | Same 9 helpers, different orchestration. Sub-modules satisfy Task B's literal "4 independent endpoints"; orchestrator recovers hard PRE_FACT gating that's only possible in single-transaction sequential execution. Demonstrable cleanliness difference in `fact_price_offer`. |
| Path A vs Path B — observable difference? | Same CSV: Path A → fact clean (gate filtered bad rows). Path B → fact contains flagged rows; analytics need `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL`. |
| Why CTI not JSONB for payment? | `schema.sql` SECTION 2 (above `payment_type_enum`) |
| Why monthly partition not daily? | `schema.sql` partitioning block (~line 470) |
| Why three DQ stages, not two? | `task_b_answers.md` B.1 + `task_c_answers.md` C.2 |
| Why SCD-2 not SCD-1? | `schema.sql` `fact_partner_price_history` block |
| Why structured matching, not embeddings? | `harmonise/scorer.py` docstring + `task_b_answers.md` B.4 |
| Why FX rate frozen per fact row? | `schema.sql` `fact_price_offer.fx_rate_date` comment |
| Why event-driven `dws_partner_dq_per_batch`? | `schema.sql` `dws_partner_dq_per_batch` block |
| How does anomaly suppression work? | `dim_market_event` + `fact_anomaly.suppression_*` columns |
| How would you calibrate weights? | Logistic Regression over labelled anomalies, with `dim_anomaly_threshold` storing source = 'data_calibrated' |
| Show me a HIGH severity anomaly | Run `/detect-anomalies` in Swagger UI (after injecting baseline per `demo_queries.sql` §5) |
| What if a partner cuts data feed? | Replay via `source_batch_id` — surgical, not full reload |
| What if Partner C is JSON? | Add a parser at `_step1_csv_to_staging`; downstream pipeline reads `stg_price_offer` so unchanged |
| What if Apple ships a new product line (Vision Pro)? | `task_c_answers.md` C.1 — Catalog team adds to Product Reference, re-seed `dim_product_model`, replay batch |

---

## Slide 22 — Technical Glossary

### Q1 — How does a country value flow through the pipeline?

```
Partner CSV value: "NZ" or "New Zealand"
        ▼
   _COUNTRY_NAME_MAP  (services.py)
       "NZ"          → "NZ"
       "New Zealand" → "NZ"
        ▼
   stg_price_offer.country_code = 'NZ'
        ▼
   fact_price_offer.country_code = 'NZ'
```

Fact tables store ISO codes (2 chars vs 11) for storage efficiency at scale; presentation layer joins `dim_country` for the human-readable name.

### Q2 — JSONB and ENUM, when?

| JSONB use case | Why |
|---|---|
| `dq_bad_records.raw_payload` | Preserves the full original CSV row even if columns failed to parse |
| `fact_anomaly.threshold_snapshot` | Freezes `dim_anomaly_threshold` values at detection time (replay-safe) |
| `fact_anomaly.baseline_snapshot` | Freezes the per-product baseline used at detection |

| ENUM | Used in |
|---|---|
| `payment_type_enum` | CTI discriminator |
| `harmonise_confidence_enum` | Score-bucket label |
| `bad_record_status_enum` | Workflow state |
| `product_lifecycle_enum` | Anomaly sensitivity per product |

**Why ENUM beats VARCHAR + CHECK:** typo rejection at engine level; 4-byte storage; ordered (NEW < STABLE < LEGACY < EOL).

### Q3 — Async queue, Pydantic 2, asyncpg

- **Pydantic 2** — data validation library (Rust core, 5–50× faster than 1.x). Defines request/response shapes; auto-validates inputs; powers the `/docs` schema.
- **asyncpg** — non-blocking PostgreSQL driver for `asyncio`. ~1000 connections per worker (vs ~10 in psycopg2); 3–5× faster.
- **Async queue** — "to-do list" producers append to and workers consume from (Redis / SQS / RabbitMQ / Kafka). Decouples HTTP request from work; enables horizontal scaling; supports retry semantics + back-pressure.

In the demo we keep ingest synchronous within the request handler. Production design (C-3) uses S3 + SQS + worker pool.

---

## Slide 23 — Closing

**Three takeaways:**

1. **Schema design accumulates compound interest.** Choosing CTI, SCD-2, bi-temporal facts, and partitioning early made every later decision easier — anomaly detection, scaling, multi-stakeholder support all land naturally.

2. **The DQ split is the architectural payoff.** INGEST → PRE_FACT gate → SEMANTIC isn't in the spec — it emerged from real integration testing. The result: `fact_price_offer` is trustworthy by construction. Downstream analytics never need filter views.

3. **Removing things is design too.** Three aggregate tables deleted, several FK indexes removed, single mv_baseline_staging instead of three separate tables. The final architecture is lean *because* I cut what didn't earn its place.

**Ready for Q&A.** Code is browseable in `Apple_SDE_Submission/`, structured submission in `Apple_SDE_Submission/submission/`, OpenAPI spec at `http://localhost:8000/docs`.

Thank you.
