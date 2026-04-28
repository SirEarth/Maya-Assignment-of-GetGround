# Task B — API Implementation Map

The API service is a FastAPI application with **two call paths sharing the same 9 internal step helpers**:

- **Path A — `POST /pipeline`**: orchestrator endpoint that runs the full 9-step pipeline end-to-end in interleaved order (parse → INGEST DQ → harmonise → PRE_FACT DQ → fact write **with hard gate** → SEMANTIC DQ → Slowly Changing Dimension Type 2 → anomaly detection → batch summary).
- **Path B — 4 Task-B sub-modules**: `POST /load-data`, `POST /compute-dq`, `POST /detect-anomalies`, `GET /harmonise-product`. Each is independently callable and does ONE thing (a coherent group of pipeline steps). Calling them in sequence covers all 9 steps; the PRE_FACT gate degrades to **post-hoc flagging in `dq_bad_records`** (bad rows enter fact and must be filtered via `LEFT JOIN dq_bad_records`).

Both paths reuse the same 9 helpers in [api/services.py](../api/services.py) — zero duplication. Path A exists for one-click execution with strict gating semantics; Path B exists because Task B requires 4 independently callable endpoints.

---

## The 9 step helpers (shared by both paths)

| # | Helper | What it does | Reads | Writes | Where |
|---|---|---|---|---|---|
| 1 | `parse_csv_to_stg` | Parse CSV bytes, bulk-insert raw rows (with original payload preserved as JSONB) | request body | **`stg_price_offer`** | [api/services.py:144](../api/services.py#L144) |
| 2 | `run_ingest_dq` | Call `dq_run_batch_ingest` (8 INGEST rules: null/format/required-field) + mark passing rows `dq_status='INGEST_PASSED'` | `stg_price_offer` | `dq_output`, `dq_bad_records`, `stg_price_offer.dq_status` | [api/services.py:216](../api/services.py#L216) + [dq/rules.sql](../dq/rules.sql) + [dq/rules_split.sql](../dq/rules_split.sql) |
| 3 | `harmonise_in_stg` | Run the harmoniser on each staged row (Top-1 + score), write `product_model_id`, `harmonise_score`, `harmonise_confidence` back to staging. Search query is `raw_product_name` plus non-numeric extras from `raw_payload`. | `stg_price_offer`, `dim_product_model`, `Product Ref.csv` | back into `stg_price_offer` | [api/services.py:278](../api/services.py#L278) |
| 4 | `run_prefact_dq` | Call `dq_run_batch_pre_fact` (3 PRE_FACT rules: country↔currency, partner↔country, harmonise unmatched) + mark `dq_status='PRE_FACT_PASSED'` | `stg_price_offer` (post-harmonise), `dim_partner` | `dq_output`, `dq_bad_records`, `stg_price_offer.dq_status` | [api/services.py:349](../api/services.py#L349) |
| 5 | `write_stg_to_fact(gate=…)` | Build fact rows: FX → USD conversion, payment-type child routing. **`gate=True`**: only `dq_status='PRE_FACT_PASSED'` rows enter fact (Path A). **`gate=False`**: all parseable rows enter fact (Path B) | `stg_price_offer`, `dim_currency_rate_snapshot` | **`fact_price_offer`** + `fact_payment_full_price` / `fact_payment_instalment` | [api/services.py:377](../api/services.py#L377) |
| 6 | `run_semantic_dq` | Call `dq_run_batch_semantic` (2 SEMANTIC rules on already-written fact rows: low-confidence harmonise, category sanity bounds — flag-and-keep) | `fact_price_offer` | `dq_output`, `dq_bad_records` | [api/services.py:509](../api/services.py#L509) + [dq/rules_split.sql](../dq/rules_split.sql) |
| 7 | `update_scd2` | Slowly Changing Dimension Type 2 reconciliation (single CTE: `latest → existing → changed → closed → insert`) | `fact_price_offer`, `fact_partner_price_history` | `fact_partner_price_history` | [api/services.py:516](../api/services.py#L516) |
| 8 | `detect_anomalies_for_batch` | Run all 4 anomaly detectors (STATISTICAL / TEMPORAL / CROSS_PARTNER / SKU_VARIANCE) on each new fact row and persist their union to `fact_anomaly`; severity is per-signal | `fact_price_offer`, `fact_partner_price_history`, `v_partner_price_current` | **`fact_anomaly`** | [api/services.py:599](../api/services.py#L599) |
| 9 | `write_batch_summary` | UPSERT one row per batch into `dws_partner_dq_per_batch` (counts, success rate, harmonise breakdown, unique products). Idempotent — recomputes everything from current DB state | `stg_price_offer`, `fact_price_offer`, `dq_bad_records` | **`dws_partner_dq_per_batch`** | [api/services.py:1172](../api/services.py#L1172) |

---

## B.0  `POST /pipeline` — Path A orchestrator (one-click)

**What it does.** Accepts a CSV upload + `partner_code` and runs all 9 step helpers in order **inside a single PostgreSQL transaction**:

```
1 → 2 → 3 → 4 → 5(gate=True) → 6 → 7 → 8 → 9
                       ↑
                 PRE_FACT gate hard-blocks bad rows from fact_price_offer
```

Returns aggregated `PipelineResponse` (rows_loaded, rows_bad, dq_summary, anomalies). On any step failure the whole batch rolls back — no half-loaded fact rows.

| Component | Where |
|---|---|
| HTTP route | [api/main.py:114](../api/main.py#L114) |
| Service implementation (run_pipeline) | [api/services.py:1535](../api/services.py#L1535) |
| Response model | [api/models.py](../api/models.py) — `PipelineResponse` |

---

## B.1  `POST /load-data` — Load Data (Task B sub-module #1)

**What it does.** Accepts CSV + `partner_code`. Covers pipeline **steps 1, 3, 5 (gate=False), 7, 9** — parse, harmonise (per Task B "standardise products using /harmonise-product endpoint, country codes, partner names, and timestamps"), write fact (no gate), update Slowly Changing Dimension Type 2, refresh summary. Returns HTTP 202 with `job_id`.

DQ steps (2/4/6) are NOT run here — caller invokes `/compute-dq` separately. Bad rows enter fact and are flagged post-hoc (no PRE_FACT gate). For end-to-end with hard gating, use `POST /pipeline` instead.

| Component | Where |
|---|---|
| HTTP route + 202 contract | [api/main.py:156](../api/main.py#L156) |
| Job-status poll endpoint | [api/main.py:189](../api/main.py#L189) |
| Service implementation (`submit_load_job`) | [api/services.py:1331](../api/services.py#L1331) |
| Internal helpers used | `parse_csv_to_stg` (1) → `harmonise_in_stg` (3) → `write_stg_to_fact(gate=False)` (5) → `update_scd2` (7) → `write_batch_summary` (9) |

---

## B.2  `POST /compute-dq` — Validate Data Quality (Task B sub-module #2)

**What it does.** Runs all **13 active rules** against the given `source_batch_id`. Per-rule pass rates → **`dq_output`**. Per-row violations (with original CSV row preserved as JSONB `raw_payload`) → **`dq_bad_records`**. Refreshes batch summary. Covers pipeline **steps 2, 4, 6, 9**.

Designed to be called **after** `/load-data` populates fact. PRE_FACT-failing rows are flagged post-hoc; they remain in fact and must be filtered via `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL` if downstream queries need a clean view.

**Rule coverage** (13 rules, 3 stages):
- **INGEST** (8 rules) — null checks, format, range, conditional dependencies (e.g. `payment_type=INSTALMENT ⇒ monthly_amount NOT NULL`)
- **PRE_FACT** (3 rules) — country↔currency, partner↔country, harmonise unmatched
- **SEMANTIC** (2 rules) — low-confidence harmonise, category sanity bounds

| Component | Where |
|---|---|
| HTTP route | [api/main.py:204](../api/main.py#L204) |
| Service implementation (`compute_dq_service`) | [api/services.py:1404](../api/services.py#L1404) |
| 13 PL/pgSQL rule functions + `dq_rule_catalog` registry | [dq/rules.sql](../dq/rules.sql) |
| Stage orchestrators (`dq_run_batch_ingest` / `dq_run_batch_pre_fact` / `dq_run_batch_semantic` / `dq_run_batch`) | [dq/rules_split.sql](../dq/rules_split.sql) |
| `dq_output` + `dq_bad_records` schemas | [schema.sql](../schema.sql) |
| Bad-records review API (Task C-2 hook): `GET /bad-records`, `POST /bad-records/{id}/resolve` | [api/main.py:291](../api/main.py#L291) |
| Internal helpers used | `run_ingest_dq` (2) → `run_prefact_dq` (4) → `run_semantic_dq` (6) → `write_batch_summary` (9) |

---

## B.3  `POST /detect-anomalies` — Detect Anomalies (Task B sub-module #3)

**What it does.** For each fact row in scope, compares its USD price vs 30-day rolling mean from `fact_partner_price_history` (Slowly Changing Dimension Type 2 table). Classifies severity (HIGH ≥25%, MEDIUM ≥15%, LOW ≥10%) and returns a structured visualization payload (time series + baseline band + cross-partner comparison) that the frontend can render with Chart.js / Recharts. Covers pipeline **step 8** (and refreshes step 9).

**Four implemented signals** (all writing to `fact_anomaly` with their own `anomaly_type`, `severity`, `signal_score` and `baseline_snapshot`):

| Signal | Compares against | Catches | Threshold tier |
|---|---|---|---|
| `STATISTICAL` | 30-day rolling AVG/STDDEV from `fact_partner_price_history` | "unusual for THIS product right now" | ≥10% LOW · ≥15% MEDIUM · ≥25% HIGH |
| `TEMPORAL` | last valid price for the same (product, partner, country, payment_type) | sudden jumps that the rolling mean smooths | same `pct_off` ladder as STATISTICAL |
| `CROSS_PARTNER` | median of OTHER partners' current prices via `v_partner_price_current` | one-partner pricing errors / data feed bugs | same `pct_off` ladder |
| `SKU_VARIANCE` | within-batch z-score across same-model same-day observations | per-row typos (Space Grey iPad standalone outlier) | z ≥1.5 LOW · ≥2.5 MEDIUM · ≥4.0 HIGH |

Each detector lives in its own helper (`_detect_statistical`, `_detect_temporal`, `_detect_cross_partner`, `_detect_sku_variance`) and returns an independent `Anomaly` list. The main `detect_anomalies_for_batch` concatenates them — same offer triggering N signals = N rows in `fact_anomaly` (UNIQUE constraint on `(offer_id, anomaly_type)` enforces one row per signal). Independent severity routing means HIGH on STATISTICAL doesn't dilute MEDIUM on CROSS_PARTNER.

### Expected behaviour on `Partner A.csv` + `Partner B.csv` (sample data shape)

A clean `./start.sh reset` followed by `POST /pipeline` for both partners produces these results in `fact_anomaly`:

| Signal | Triggers on sample data? | Why | When it WOULD trigger naturally |
|---|---|---|---|
| **STATISTICAL** | ❌ 0 rows | Each (product, country) has only 1 SCD-2 history row after first ingest. The detector requires `count ≥ 2` baseline samples within a 30-day window to compute mean/stddev. | Production crawls run daily/hourly → within 30 days the same product accumulates 8–30 history rows per partner → baseline becomes meaningful. |
| **TEMPORAL** | ❌ 0 rows | Needs a *prior* `valid_from_date < current crawl_ts` row for the same (product, partner, country, payment_type). On first ingest, no prior price exists. | Any subsequent crawl where a partner changes price ≥10% from its previous value. Verified in dev with a synthetic INSTALMENT spike: `iP 17 PM 512GB` jumped from $1,689 → $4,680 USD → fired `TEMPORAL HIGH` with `signal_score = 1.000`. |
| **CROSS_PARTNER** | ❌ 0 rows | The sample data has Partner A only in **AU** and Partner B only in **NZ** — `v_partner_price_current` aggregates by `(product, country, payment_type)` and so always sees `partner_count = 1`. The detector skips when there are no peer partners. | Any (product, country) combination with ≥2 partners — common in real markets where multiple resellers carry the same SKU. Verified in dev by SQL-injecting one Partner A row in NZ for an iPad model Partner B carries → fired 20 `CROSS_PARTNER HIGH` rows on the next Partner B upload (Space Grey iPad $146,340 vs peer median $900). |
| **SKU_VARIANCE** | ✅ **38 rows** (37 LOW + 1 MEDIUM) | Self-contained: groups same-model same-day same-partner observations, requires only ≥3 rows + non-zero stddev. The sample data hits this naturally because both partners have multi-color/storage SKUs of the same model on the same crawl date. | Always works on first ingest — no historical priors needed. |

**Reading this honestly.** The detectors themselves work correctly across all four signals (verified end-to-end in dev with synthetic injections, plus 3 unit tests in `api/tests/test_api.py::TestAnomalyMultiSignal`). The 0-count results for three of them on the *sample data* reflect data shape (single-country-per-partner, single ingest, no price changes) — not algorithmic gaps. In a production deployment with multiple partners overlapping countries and continuous crawling, all four signals fire naturally as designed.

| Component | Where |
|---|---|
| HTTP route | [api/main.py:241](../api/main.py#L241) |
| Service implementation (`detect_anomalies_service`) | [api/services.py:1494](../api/services.py#L1494) |
| Step helper (`detect_anomalies_for_batch`) | [api/services.py:599](../api/services.py#L599) |
| Visualization payload builder | [api/services.py:1252](../api/services.py#L1252) |
| `fact_anomaly` (with `threshold_snapshot` JSONB) + `dim_anomaly_threshold` + baseline materialised view | [schema.sql](../schema.sql) |
| Pydantic response models (signal breakdown, contextual factors, visualisation payload) | [api/models.py](../api/models.py) |

---

## B.4  `GET /harmonise-product` — Harmonise Product (Task B sub-module #4)

**What it does.** Takes a partner-supplied raw product name; returns up to *k* ranked canonical model candidates from Product Reference. Each candidate carries a 0–1 score, a confidence bucket (HIGH / MEDIUM / LOW / MANUAL), and a per-signal breakdown for explainability.

**Algorithm.** Three-signal hybrid: `0.5 × attr_match + 0.3 × token_jaccard + 0.2 × char_fuzz`, plus a **structural override** that upgrades to HIGH whenever `attr_match ≥ 0.95` (avoids manual-review overload on partner verbosity / missing-suffix cases like `iP15P 128`).

This same harmoniser is invoked internally by step helper 3 (`harmonise_in_stg`) for every staged row during `/load-data` and `/pipeline`. When called from step 3 the search query is augmented with non-numeric `raw_payload` values (excluding metadata like `CRAWL_TS`, `COUNTRY_VAL`, `PARTNER`, and the structured price fields), so partners that split product attributes into their own columns (e.g. a separate `CONNECTIVITY=WiFi`) feed those hints into the harmoniser without changing the schema.

| Component | Where |
|---|---|
| HTTP route | [api/main.py:264](../api/main.py#L264) |
| Service entry | [api/services.py:79](../api/services.py#L79) |
| Tokenisation + abbreviation expansion | [harmonise/normaliser.py](../harmonise/normaliser.py) |
| Attribute extraction (category, model_line, chip, storage_gb, connectivity) | [harmonise/extractor.py](../harmonise/extractor.py) |
| Three-signal scorer | [harmonise/scorer.py](../harmonise/scorer.py) |
| Top-K orchestration + structural override | [harmonise/harmoniser.py](../harmonise/harmoniser.py) |
| Abbreviation dictionary (Layer 1 manual; Layer 2 mining + Layer 3 business loop scaffolded) | [harmonise/dictionary.py](../harmonise/dictionary.py) |
| Tests (22 cases) | [harmonise/tests/](../harmonise/tests/) |

---

## Path A vs Path B — semantic difference

| | Path A `POST /pipeline` | Path B sub-modules in sequence |
|---|---|---|
| Step coverage | 9 / 9 (interleaved) | 9 / 9 (grouped) |
| PRE_FACT gate | **Hard-block**: bad rows do NOT enter `fact_price_offer` | **Post-hoc flag**: bad rows enter fact, marked in `dq_bad_records` |
| `fact_price_offer` cleanliness | Trustworthy by definition; analytics queries can hit fact directly | Contains flagged rows; analytics queries need `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL` |
| Slowly Changing Dimension Type 2 input | Only PRE_FACT-passing rows | All loaded rows (history may include flagged values) |
| Transactional scope | Single PostgreSQL transaction over all 9 steps | Three transactions (one per endpoint) |
| Use case | Production one-click ingest with strong guarantees | Debugging, ad-hoc DQ re-run, granular pipeline observation |

**Why both exist.** Task B requires 4 independently callable endpoints (Path B). The orchestrator endpoint (Path A) exists because hard PRE_FACT gating is fundamentally only possible inside a single-process sequential execution — `/load-data` must be independently callable, so it cannot wait for `/compute-dq` to decide whether to write fact. The trade-off is documented; demos show both paths against the same dataset to make the difference observable.

---

## Verification

- **47 / 47 automated tests passing** ([api/tests/](../api/tests/) + [harmonise/tests/](../harmonise/tests/)) — covers Path A end-to-end, all 4 Path B sub-modules, and Path-parity (both paths cover the same 9 steps)
- Interactive API docs at `http://localhost:8000/docs` (Swagger UI) and `/redoc`
- OpenAPI 3.x spec exported to [submission/api_openapi.json](api_openapi.json)
- End-to-end smoke test commands in [README.md](../README.md)
