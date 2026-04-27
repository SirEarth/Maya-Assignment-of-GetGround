# Pricing Pipeline — Project Presentation

> 1-hour project introduction & live demo
> 30-minute Q&A + design / data-model / code review
>
> Prepared by: huizhongwu

**Slide deck flows in 7 sections matching [`demo_guide.md`](demo_guide.md):**

| § | Topic | Slides | Time |
|---|---|---|---:|
| 1 | Project context & what I delivered | 1–3 | 3 min |
| 2 | Live app walkthrough | 4 | 10 min |
| 3 | Technical architecture & pipeline diagrams | 5–8 | 8 min |
| 4 | Task B endpoint design deep-dive | 9–13 | 15 min |
| 5 | Task C-3 scaling to 1 M records | 14 | 5 min |
| 6 | Reflection — challenges / highlights / next steps | 15–17 | 8 min |
| 7 | Q&A | 18–20 | 30 min |

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

**Task B — 4 endpoints (✅ all implemented, real PostgreSQL backend):**
- `GET /harmonise-product` — Top-K with score breakdown
- `POST /load-data` — 8-step ingest pipeline in single transaction
- `POST /compute-dq` — 13 PL/pgSQL rules across 3 stages
- `POST /detect-anomalies` — multi-signal + structured visualization payload

**Task C — 3 short write-ups (✅ in `submission/task_c_answers.md`):**
- C-1 data model adaptation when new partners onboard
- C-2 DQ + business correction loop
- C-3 scaling `/load-data` to 1 M records

**Quality bar:** **39 / 39 automated tests passing** (22 harmonise unit + 17 API integration).

**Architectural innovation beyond the spec:** **3-stage DQ with severity-driven policy** (INGEST → PRE_FACT gate → SEMANTIC) — makes `fact_price_offer` trustworthy by construction.

---

# §2 · Live App Walkthrough (10 min)

---

## Slide 4 — Live Demo Plan (Swagger UI + VSCode)

**Goal:** show the full pipeline end-to-end on real PostgreSQL — entirely in the browser via Swagger UI, plus VSCode for SQL inspection. **No terminal / curl needed.**

| Step | Where | What |
|---|---|---|
| 1 | Browser (Swagger UI) | Scroll through all 8 endpoints; Pydantic auto-generated, nothing handwritten |
| 2 | Swagger UI → `POST /load-data` | "Try it out" → upload `Partner A.csv` + `partner_code=PARTNER_A` → Execute. Copy `source_batch_id` from the response panel. |
| 3 | Swagger UI → `POST /load-data` again | Upload `Partner B.csv` → tell the **NZ story** (154 rows; resolver fix; replay loop) |
| 4 | Browser → `results_showcase.html` | Open the static visual dashboard; walk through the 7 cards: headline numbers → pipeline funnel (with the 9 Apple Watch SKUs blocked story) → harmonise confidence pie → DQ pass-rate bars → anomaly time-series with band → sample harmonise breakdowns → sample bad records → NZ 154-rows iteration story |
| 5 | (Optional) VSCode Database Panel | One quick `demo_queries.sql §0` sanity check to prove the dashboard reflects live DB, not a pre-rendered mock. Other SQL sections kept as drill-down for Q&A follow-ups. |

**Expected baseline numbers after both uploads:**

| Table | Rows |
|---|---:|
| `stg_price_offer` | ~4 208 |
| `fact_price_offer` | ~4 174 |
| `fact_payment_full_price` / `fact_payment_instalment` | 1 066 / 3 108 |
| `fact_partner_price_history` | 120 |
| `dq_bad_records` | ~188 |
| `dq_output` | 26 (= 2 batches × 13 rules) |

**Punchline to land at end:** *"Notice the funnel — every stg row preserved with `raw_payload`, but only PRE_FACT-passing rows enter `fact_price_offer`. Anything HIGH severity is blocked here. So `SELECT * FROM fact_price_offer` is safe to query directly — analytics never need a filter view."*

---

# §3 · Technical Architecture & Pipeline Diagrams (8 min)

---

## Slide 5 — Architecture (3-Layer)

```
┌────────────────────────────────────────────────────────────────────┐
│                         Client / Interviewer                       │
│            Swagger UI · curl · future Frontend (React)             │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ HTTP/JSON
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  API Layer · FastAPI + Pydantic 2 (api/main.py)                    │
│   /load-data    /compute-dq    /detect-anomalies                   │
│   /harmonise-*  /bad-records   /load-data/{id}                     │
└──────────────────────────────┬─────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  Service Layer · Async Python (api/services.py)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐    │
│  │  Harmoniser  │  │ DQ runtime   │  │ Anomaly detector       │    │
│  │ (in-memory   │  │ (calls SQL   │  │ (statistical baseline  │    │
│  │  Top-K)      │  │  functions)  │  │  + visualization)      │    │
│  └──────────────┘  └──────────────┘  └────────────────────────┘    │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ asyncpg pool
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  Data Layer · PostgreSQL 14+                                       │
│                                                                    │
│  ▸ Dimensions (14): dim_country / dim_partner / dim_product_*      │
│                     dim_currency_rate_snapshot · dim_anomaly_*     │
│  ▸ Facts (5):       fact_price_offer (CTI parent)                  │
│                     fact_payment_full_price · fact_payment_*       │
│                     fact_partner_price_history (SCD-2)             │
│                     fact_anomaly                                   │
│  ▸ DQ (3):          dq_rule_catalog (13 rules) · dq_output         │
│                     dq_bad_records (workflow + raw_payload JSONB)  │
│  ▸ DWS:             materialized view + summary tables             │
└────────────────────────────────────────────────────────────────────┘
```

**Why this split:**
- **API layer** is thin — Pydantic auto-validates inputs and auto-generates OpenAPI; no separate spec to maintain.
- **Service layer** holds pure-Python algorithms (Harmoniser singleton + Anomaly detector) and orchestrates SQL calls.
- **Data layer** is PostgreSQL with a 29-table star schema; asyncpg pool gives concurrent queries without thread overhead.

---

## Slide 6 — Business Pipeline (Sync Ingest + Async Anomaly Detection)

```
   ═══════ /load-data sync pipeline (single PostgreSQL transaction) ═══════

   Partner CSV upload
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
   ④ PRE_FACT DQ (3 rules) — ⚠️ HIGH-severity GATE
           │  country↔currency / partner↔country / harmonise unmatched
           │  failing rows → dq_bad_records, NEVER enter fact
           │  passing rows → dq_status='PRE_FACT_PASSED'
           ▼
   ⑤ Build facts — fact_price_offer (parent) + fact_payment_* (CTI children)
           │
           ▼
   ⑥ SEMANTIC DQ (2 rules) — single-row soft signals (low-confidence
           │  harmonise, category sanity bounds). flag-and-keep policy.
           ▼
   ⑦ SCD-2 reconciliation — single CTE: latest → existing → changed
           │                              → closed → insert
           │  → fact_partner_price_history
           ▼
   ⑧ Batch summary → dws_partner_dq_per_batch
           │
           │  transaction commit
           ▼
   ═════════ Async downstream (on-demand / scheduled trigger) ═════════════

           POST /detect-anomalies (multi-signal detector)
                   │
                   │  reads:
                   │   • fact_partner_price_history (history baseline)
                   │   • dws_product_price_baseline_1d (rolling stats)
                   │   • dim_anomaly_threshold (thresholds)
                   │   • dim_market_event (suppression windows)
                   ▼
           4-signal scoring (each independent severity):
              STATISTICAL · TEMPORAL · CROSS_PARTNER · SKU_VARIANCE
                   │
                   │ each triggered signal = one fact_anomaly row
                   │ visualization payload (series + band + cross-partner)
                   ▼
           fact_anomaly  ───►  dim_alert_policy routing
                                 HIGH  → Slack / Teams (immediate)
                                 MED   → Email digest (daily)
                                 LOW   → UI only
```

**Key design call:** the 8 sync steps run inside **one PostgreSQL transaction** — any failure rolls back the whole batch. Anomaly detection is **decoupled** as a separate API call so it can be re-run with tuned thresholds without re-ingesting.

**Three DQ stages, three policies:**

| Stage | Where | Policy | Why |
|---|---|---|---|
| INGEST | raw stg | Block at staging | Parse / format errors don't deserve fact |
| **PRE_FACT** | enriched stg | **Block from fact** | Factual errors pollute analytics |
| SEMANTIC | fact | Flag-and-keep | Soft signals need business judgment |

---

## Slide 7 — Data Model: Star Schema (Kimball)

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

## Slide 8 — Compressed Price History + Aggregation (SCD-2 + A+MV)

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

# §4 · Task B Endpoint Design (15 min)

---

## Slide 9 — API Surface

| Endpoint | Method | Purpose | Implementation |
|----------|--------|---------|----------------|
| `/load-data` | POST | Submit CSV, run 8-step pipeline | `api/services.py:122` |
| `/load-data/{job_id}` | GET | Poll progress | `api/services.py:get_job_status` |
| `/harmonise-product` | GET | Top-K canonical match + score | `harmonise/` (6 modules) |
| `/compute-dq` | POST | Run DQ rules on a batch | `api/services.py:compute_dq` + `dq/rules.sql` |
| `/detect-anomalies` | POST | Multi-signal anomaly detection | `api/services.py:detect_anomalies` |
| `/bad-records` | GET | List flagged records | `api/services.py:list_bad_records` |
| `/bad-records/{id}/resolve` | POST | Resolve + optionally replay | `api/services.py:resolve_bad_record` |
| `/health` | GET | Liveness probe | `api/main.py:health` |

**Built with FastAPI + Pydantic 2:** typed contracts everywhere; auto-generated OpenAPI 3.x at `/docs` (Swagger UI) and `/redoc`.

**Tested:** 39 automated tests (22 harmonise unit + 17 API integration via FastAPI TestClient with active lifespan).

---

## Slide 10 — `GET /harmonise-product`

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

## Slide 11 — `POST /load-data`

**The 8-step pipeline (see Slide 6 diagram).** This is the centrepiece — five design calls worth highlighting:

**1. Single PostgreSQL transaction.** All 8 steps wrap inside `async with conn.transaction():`. Any failure rolls back the whole batch — no half-loaded state, idempotent re-tries.

**2. Three-stage DQ with severity policy.** INGEST blocks at stg; **PRE_FACT blocks at the gate before fact insert** (the architectural innovation); SEMANTIC flags-and-keeps after fact write.

**3. Class Table Inheritance for payment.** `fact_price_offer` has a `payment_type_enum` discriminator; `fact_payment_full_price` and `fact_payment_instalment` are 1:1 child tables with their own NOT NULL + CHECK constraints (impossible in a sparse single-table design).

**4. Currency frozen on fact row.** Step ⑤ JOINs `dim_currency_rate_snapshot` once at load time and stores `effective_total_local`, `effective_total_usd`, `fx_rate_to_usd`, `fx_rate_date` on the fact. Future FX corrections don't retroactively change historical USD values.

**5. SCD-2 reconciliation in one CTE.** Step ⑦ uses `latest → existing → changed → closed → insert` in a single SQL statement. No row-by-row Python.

**Returns HTTP 202 + `job_id`** — matches the C-3 async contract even though the demo runs synchronously. Production path: enqueue, parallel workers, ~90 sec for 1 M rows.

---

## Slide 12 — `POST /compute-dq` (DQ 3-Stage Strategy)

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

## Slide 13 — `POST /detect-anomalies`

**Four anomaly types — each detected independently:**

| Type | Question answered | Status |
|------|-------------------|:---:|
| STATISTICAL | "Is this price outside historical norms?" | ✅ End-to-end |
| TEMPORAL | "Did the price suddenly change?" | 🟡 Designed |
| CROSS_PARTNER | "Is this price way off from peers?" | 🟡 Designed |
| SKU_VARIANCE | "Is the spread within one batch suspicious?" | 🟡 Designed |

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

# §5 · Task C-3 — Scaling to 1 Million Records (5 min)

---

## Slide 14 — Scaling to 1 M Records

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

See [`task_c_answers.md`](../Apple%20SDE/submission/task_c_answers.md) C.3 for full details.

---

# §6 · Reflection (8 min)

---

## Slide 15 — Challenges & Iterations

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

## Slide 16 — Design Highlights I'm Proud Of

**1. Three-stage DQ with severity-driven policy.** The architectural call I'd defend in any review:
   - INGEST stops at staging (parse errors)
   - PRE_FACT blocks factual errors from entering fact
   - SEMANTIC flags soft signals after fact write

   **Result:** `fact_price_offer` is trustworthy by construction. Downstream analytics queries don't need filter views guarding against unresolved HIGH severity records.

**2. DQ rules are PL/pgSQL functions.** 13 rules executed in PostgreSQL, not Python. **One DB call replaces 13 M Python checks** at 1 M rows. Plus: `dq_rule_catalog` is metadata-driven — adding a rule = one INSERT, no code change.

**3. Explainable harmonise.** Three signals + structured override → every match has a transparent breakdown. Business reviewers can see *why* the matcher decided something, which is invaluable for triage. Vector embeddings would have been a black box.

**4. Visualization payload decoupled from rendering.** `/detect-anomalies` returns structured JSON, not images. Same payload feeds Chart.js dashboards, Slack alert cards, PDF reports — three consumers from one definition.

**5. `fact_anomaly` one-row-per-signal (not per-offer).** An offer that trips multiple signals appears as multiple rows, each routable to a different team. Combining them into a composite would dilute or hide individual concerns.

**6. Adapting to a new partner is a configuration change, not a migration.** Adding Partner C = INSERT one row in `dim_partner`. Star-schema decoupling pays off.

| Change | What happens | Schema impact |
|--------|--------------|---------------|
| New partner, existing payment scheme | `INSERT INTO dim_partner` | **None** |
| New payment type (e.g. BNPL) | `ALTER TYPE payment_type_enum ADD VALUE`; `CREATE TABLE fact_payment_bnpl` | One new child table |
| New country | `INSERT INTO dim_country` + `dim_timezone` | **None** |
| Unknown product category | `DQ_HARM_002` flags → review → catalog adds to Reference → replay | **None on facts** |

---

## Slide 17 — What I'd Build Differently

**1. Three remaining anomaly signals.** TEMPORAL / CROSS_PARTNER / SKU_VARIANCE — schemas and response shapes are in place; their detector branches are scoped as future work. The visualization helper is signal-agnostic and reusable.

**2. Async pipeline + COPY.** C-3 is the production gap. For the take-home demo, sample data finishes in seconds; for 1 M rows, swapping in `COPY` (1 day) plus adding `ingest_job` table + worker pool would deliver the ~90 sec target.

**3. Harmonise Layer 2 — data-driven mining.** Currently only Layer 1 (manual dictionary) and Layer 3 (business loop). Layer 2 (TF-IDF + N-gram co-occurrence over Product Ref Long/Short Description alignment) is scaffolded but not implemented.

**4. Sentence-transformer fallback.** Pluggable via `score_fn="embedding"` — not enabled because at 281 reference rows it adds dependency without measurable benefit. Worth revisiting if Reference grows 10×.

**5. Real-time alerting via Kafka / Webhooks.** Current design has Postgres `LISTEN/NOTIFY` trigger as a placeholder pattern; production would be Kafka producer or webhook fan-out via `dim_alert_channel`.

**6. Observability — `ingest_job` + Prometheus metrics.** Currently relies on `dws_partner_dq_per_batch` for batch-level KPIs. Production would add per-job duration / chunk progress / retry counts to a dedicated metrics endpoint.

---

# §7 · Q&A (30 min)

---

## Slide 18 — Q&A Cheat Sheet

**Likely deep-dive questions and where the answer lives:**

| Question | Where to look |
|----------|---------------|
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

## Slide 19 — Technical Glossary

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

## Slide 20 — Closing

**Three takeaways:**

1. **Schema design accumulates compound interest.** Choosing CTI, SCD-2, bi-temporal facts, and partitioning early made every later decision easier — anomaly detection, scaling, multi-stakeholder support all land naturally.

2. **The DQ split is the architectural payoff.** INGEST → PRE_FACT gate → SEMANTIC isn't in the spec — it emerged from real integration testing. The result: `fact_price_offer` is trustworthy by construction. Downstream analytics never need filter views.

3. **Removing things is design too.** Three aggregate tables deleted, several FK indexes removed, single mv_baseline_staging instead of three separate tables. The final architecture is lean *because* I cut what didn't earn its place.

**Ready for Q&A.** Code is browseable in `Apple SDE/`, structured submission in `Apple SDE/submission/`, OpenAPI spec at `http://localhost:8000/docs`.

Thank you.
