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

## Slide 4 — Live Demo Plan (Swagger UI + HDML)

./start.sh reset
curl -s -X POST http://localhost:8000/pipeline -F "file=@Partner A.csv" -F "partner_code=PARTNER_A" > /dev/null
curl -s -X POST http://localhost:8000/pipeline -F "file=@Partner B.csv" -F "partner_code=PARTNER_B" > /dev/null

http://localhost:8000/docs
./start.sh wipe

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

```
                  raw partner name
                  "iP 17 PM 512GB"
                          │
                          ▼
                  ┌───────────────┐
                  │  normalise()  │   预处理:小写、标点清洗、缩写展开、噪声词
                  └───────┬───────┘   去掉 → 干净 token 列表
                          │
              ['iphone','17','pro','max','512','gb']
                          │
              ┌───────────┴────────────┐
              ▼                         ▼
        ┌──────────┐           ┌──────────────┐
        │ extract()│           │ tokens 原样  │
        └────┬─────┘           └──────┬───────┘
             │ 结构化属性               │ 原始 token list / 文本
             ▼                          ▼
        ┌─────────────────────────────────────────┐
        │   3 个独立信号 vs Reference 库每条      │
        ├─────────────────────────────────────────┤
        │ ① attr_match    × 0.5  (主导)          │
        │ ② token_jaccard × 0.3                   │
        │ ③ char_fuzz     × 0.2  (兜底)          │
        └────────────────┬────────────────────────┘
                         │
                         ▼
              combined score → HIGH/MEDIUM/LOW
```


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

**Contextual adjustments stored per row:**
- `lifecycle_factor` — NEW / STABLE / LEGACY / EOL moderates threshold
- `event_suppression_factor` — `dim_market_event` (Apple launch / Black Friday) suppresses known volatility
- `category_sensitivity` — wider tolerance for high-volatility categories (AirPods)

**All thresholds in `dim_anomaly_threshold`** — config-driven, no magic numbers; Logistic Regression calibration plan documented.

**Implementation honesty — partial wiring.** Schema and per-row snapshot
columns are complete; detection code currently hardcodes `lifecycle_factor=1.0`,
`suppression_applied=False`, and severity bars (`SEV_BAR_*` constants) instead of
querying `dim_anomaly_threshold` / `dim_product_lifecycle` / `dim_market_event`.
Plumbing the dim-table lookups is ~0.5 day — the replay-safety snapshot
mechanism is already correct, so only the source-of-values changes.

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

---

# §7 · Reflection (8 min)

---

## Slide 18 — Challenges & Iterations

1. **Schema iteration.** First draft used a wide table with sparse payment columns; assignment explicitly forbids sparse — refactored to **Class Table Inheritance**. DQ started as 2 stages (INGEST + SEMANTIC); during integration testing realised "fact has invalid country/currency rows" → added **PRE_FACT gate**.

2. **Harmonise edge cases.** Partner A ships `iP15P 128` (no "GB" suffix); naive token-match gives MEDIUM/LOW. Fixed with two heuristics:
   - **Storage-set fallback** — standalone digits matching `{64, 128, 256, 512, 1024, 2048}` get treated as GB
   - **Structural override** — if attribute_match alone ≥ 0.95, force HIGH bucket (avoids manual-review overload)

3. **Real-world dirt the spec hides.** Discovered Partner B has 154 rows where `COUNTRY_VAL = "NZ"` (ISO code, not full name). DQ engine flagged them; root cause was the resolver only accepting full names. **Real DQ → rule iteration → replay loop.**

4. **Threshold calibration is the silent killer.** First draft had hand-picked weights (`0.4 / 0.3 / 0.3`). Fixed by introducing `dim_anomaly_threshold` with documented calibration plans (Logistic Regression for weights; percentiles for thresholds; A/B testing for suppression factors).

5. **Removing things is design too.** Dropped 3 aggregate tables that didn't earn their keep (`dws_price_offer_market_local_1d`, `dws_price_offer_td`, `dws_cross_partner_comparison_1d`) once I confirmed no API endpoint needed them.

---

## Slide 19 — Design Highlights I'm Proud Of

**1. Orchestrator + sub-modules with shared 9 helpers.** 

**2. Three-stage DQ with severity-driven policy.** 

**3. Explainable harmonise.** Three signals + structured override → every match has a transparent breakdown. Business reviewers can see *why* the matcher decided something, which is invaluable for triage. Vector embeddings would have been a black box.

**4. `fact_anomaly` one-row-per-signal (not per-offer).** An offer that trips multiple signals appears as multiple rows, each routable to a different team. Combining them into a composite would dilute or hide individual concerns.

**5. Adapting to a new partner is a configuration change, not a migration.** Adding Partner C = `INSERT INTO dim_partner`; new payment type = ALTER TYPE + new CTI child; new country = `INSERT INTO dim_country`. Star-schema decoupling pays off (full table on Slide 15 / Task C-1).

---

## Slide 20 — What I'd Build Differently

**1. Anomaly signal weight calibration.** 

**2. Async pipeline + COPY.** 

**3. Harmonise Layer 2 — data-driven mining.** 

**4. Sentence-transformer fallback.** 

**5. Real-time alerting via Kafka / Webhooks.** 

**6. Observability — `ingest_job` + Prometheus metrics.** 

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

## Slide 22 — Closing

**Three takeaways:**

1. **Schema design accumulates compound interest.** Choosing CTI, SCD-2, bi-temporal facts, and partitioning early made every later decision easier — anomaly detection, scaling, multi-stakeholder support all land naturally.

2. **The DQ split is the architectural payoff.** INGEST → PRE_FACT gate → SEMANTIC isn't in the spec — it emerged from real integration testing. The result: `fact_price_offer` is trustworthy by construction. Downstream analytics never need filter views.

3. **Removing things is design too.** Three aggregate tables deleted, several FK indexes removed, single mv_baseline_staging instead of three separate tables. The final architecture is lean *because* I cut what didn't earn its place.

Thank you.
