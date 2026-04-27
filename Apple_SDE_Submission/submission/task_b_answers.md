# Task B — API Implementation Map

Concise summary of what each Task B endpoint does and where its code lives.
Endpoint order follows the assignment brief: Load Data → Validate Data Quality → Detect Anomalies → Harmonise Product.

---

## B.1  `POST /load-data` — Load Data

**What it does.** Accepts a CSV upload + `partner_code`. Parses it, standardises product names (via the harmoniser), country codes, partner names, and timestamps, then writes the meaningful rows into `fact_price_offer`. Returns HTTP 202 with a `job_id`. Runs as a single Postgres transaction — any failure rolls back the whole batch.

**Pipeline (8 steps, in order):**

| # | Step | Reads | Writes | Where |
|---|---|---|---|---|
| 1 | Parse CSV → staging table (raw payload preserved as JSONB) | request body | **`stg_price_offer`** | [api/services.py:248-322](../api/services.py#L248-L322) |
| 2 | INGEST-stage DQ (8 rules) on raw staging — null / format / range / conditional checks | `stg_price_offer` | `dq_output`, `dq_bad_records` | [dq/rules.sql](../dq/rules.sql), orchestrator `dq_run_batch_ingest` in [dq/rules_split.sql](../dq/rules_split.sql) |
| 3 | Harmonise raw product names (only rows that passed step 2) | `stg_price_offer`, `Product Ref.csv` | back into `stg_price_offer` (`product_model_id`, `harmonise_score`, `harmonise_confidence`) | [api/services.py:323-375](../api/services.py#L323-L375) |
| 4 | **PRE_FACT-stage DQ (3 rules)** — HIGH-severity gate on enriched stg: country↔currency match, partner↔country match, harmonise unmatched. **Failing rows do NOT enter fact_price_offer** — they stay in stg with `dq_status='INGEST_PASSED'` and are flagged in `dq_bad_records` for business review. | `stg_price_offer` (post-harmonise), `dim_partner` | `dq_output`, `dq_bad_records`, updates `stg_price_offer.dq_status` | orchestrator `dq_run_batch_pre_fact` in [dq/rules_split.sql](../dq/rules_split.sql); 3 stg-querying check functions in [dq/rules.sql](../dq/rules.sql) |
| 5 | Build & insert facts from PRE_FACT-passing rows: FX, USD conversion, payment-type child routing | `stg_price_offer` WHERE `dq_status='PRE_FACT_PASSED'`, `dim_currency_rate_snapshot` | **`fact_price_offer`** + `fact_payment_full_price` / `fact_payment_instalment` | [api/services.py:376-495](../api/services.py#L376-L495) |
| 6 | SEMANTIC-stage DQ (2 rules) on the **already-written** fact rows — soft signals only (low-confidence harmonise, category sanity bounds). Failing rows STAY in fact, just flagged. | `fact_price_offer` | `dq_output`, `dq_bad_records` | orchestrator `dq_run_batch_semantic` in [dq/rules_split.sql](../dq/rules_split.sql) |
| 7 | Slowly Changing Dimension Type 2 history reconciliation (single CTE: `latest → existing → changed → closed → insert`) | `fact_price_offer`, `fact_partner_price_history` | `fact_partner_price_history` | [api/services.py:496-558](../api/services.py#L496-L558) |
| 8 | Per-batch summary | `fact_price_offer`, `dq_output` | `dws_partner_dq_per_batch` | [api/services.py:559-626](../api/services.py#L559-L626) |

| HTTP route + 202 contract | [api/main.py:100-134](../api/main.py#L100-L134) |
|---|---|
| Job-status poll endpoint | [api/main.py:140-148](../api/main.py#L140-L148) |
| Top-level transaction wrapper | [api/services.py:122-247](../api/services.py#L122-L247) |

**Key ordering rationale.**
- INGEST DQ (step 2) runs **before** harmonise — skip wasted matching on malformed rows.
- PRE_FACT DQ (step 4) is a **gate before fact insert**: rules that catch *factual errors* (wrong country/currency, partner mismatch, harmonise unmatched) keep bad rows out of `fact_price_offer` entirely. The fact table is therefore trustworthy by definition — downstream analytics queries don't need filter-views to exclude unresolved HIGH severity records.
- SEMANTIC DQ (step 6) runs **after** fact write, on the canonical post-load representation. It only checks single-row *soft signals* (low confidence, category sanity bounds) where a flag-but-keep policy makes sense — business reviews these via `/bad-records`. Cross-row pricing patterns (variance, temporal jumps, cross-partner divergence) live in `/detect-anomalies`, not here.
- SCD-2 (step 7) reads only the freshly-written facts.

**Severity policy mapping.** HIGH severity rules block from fact (PRE_FACT). MEDIUM/LOW rules flag in fact (SEMANTIC). The split is data-driven via `dq_rule_catalog.target_stage` — adding a new gate rule = one catalog INSERT, no code change.

---

## B.2  `POST /compute-dq` — Validate Data Quality

**What it does.** Runs all active rules in `dq_rule_catalog` against a given `source_batch_id`, writes per-rule pass rates to `dq_output` and per-record violations to `dq_bad_records` (with `raw_payload` JSONB preserving the original row). Optionally restrict by `rules` and `stages` (INGEST / PRE_FACT / SEMANTIC).

**Rule coverage.** 13 rules across 6 categories — null checks, format, range, conditional dependencies (e.g. `payment_type=INSTALMENT ⇒ monthly_amount NOT NULL`), harmonise confidence, cross-field consistency, and category sanity.

| Component | Where |
|---|---|
| HTTP route | [api/main.py:157-177](../api/main.py#L157-L177) |
| Service implementation | [api/services.py:654-733](../api/services.py#L654-L733) |
| 13 PL/pgSQL rule functions + `dq_rule_catalog` registry + master orchestrator | [dq/rules.sql](../dq/rules.sql) |
| Stage-split orchestrators (`dq_run_batch_ingest` / `dq_run_batch_pre_fact` / `dq_run_batch_semantic`) | [dq/rules_split.sql](../dq/rules_split.sql) |
| `dq_output` + `dq_bad_records` schemas | [schema.sql](../schema.sql) |
| Bad-records review API (Task C-2 hook): `GET /bad-records`, `POST /bad-records/{id}/resolve` | [api/main.py:213-256](../api/main.py#L213-L256) |

---

## B.3  `POST /detect-anomalies` — Detect Anomalies

**What it does.** Runs four independent signals — **STATISTICAL** (vs historical baseline), **TEMPORAL** (vs last known price), **CROSS_PARTNER** (vs other partners on the same model), **SKU_VARIANCE** (price spread across SKUs of the same model). Each triggered signal is its own row in `fact_anomaly` with its own severity, so an offer that trips multiple signals appears as multiple anomaly entries (each routable to the appropriate team). All thresholds come from `dim_anomaly_threshold` and are frozen into the response (`threshold_snapshot`) for replay.

**Visualisation contract.** Returns structured JSON — time series + baseline band + anomaly markers + cross-partner bar — for the client to render with Chart.js / Recharts. Backend doesn't render images, preserving interactivity.

| Component | Where |
|---|---|
| HTTP route | [api/main.py:187-205](../api/main.py#L187-L205) |
| Service implementation | [api/services.py:811-932](../api/services.py#L811-L932) |
| `fact_anomaly` (with `threshold_snapshot` JSONB) + `dim_anomaly_threshold` + baseline materialised view | [schema.sql](../schema.sql) |
| Pydantic response models (signal breakdown, contextual factors, visualisation payload) | [api/models.py](../api/models.py) |

**Implementation status.** STATISTICAL signal wired end-to-end with full visualization payload (time series + baseline band + cross-partner comparison) populated for every triggered anomaly. TEMPORAL / CROSS_PARTNER / SKU_VARIANCE signals share the same response shape; their detector branches are scoped as future work — see comments on `AnomalyType` in [api/models.py](../api/models.py). The visualization helper [`_build_anomaly_visualization`](../api/services.py#L734) is signal-agnostic and reused as those signals are added.

---

## B.4  `GET /harmonise-product` — Harmonise Product

**What it does.** Takes a partner-supplied raw product name and returns up to *k* ranked canonical model candidates from Product Reference, each with a 0–1 score, a confidence bucket (HIGH / MEDIUM / LOW / MANUAL), and a per-signal breakdown for explainability.

**Algorithm.** Three-signal hybrid: `0.5 × attr_match + 0.3 × token_jaccard + 0.2 × char_fuzz`, plus a **structural override** that upgrades to HIGH whenever `attr_match ≥ 0.95` (avoids manual-review overload on partner verbosity / missing-suffix cases like `iP15P 128`).

| Component | Where |
|---|---|
| HTTP route | [api/main.py:76-93](../api/main.py#L76-L93) |
| Service entry | [api/services.py:63-99](../api/services.py#L63-L99) |
| Tokenisation + abbreviation expansion | [harmonise/normaliser.py](../harmonise/normaliser.py) |
| Attribute extraction (category, model_line, chip, storage_gb, connectivity) | [harmonise/extractor.py](../harmonise/extractor.py) |
| Three-signal scorer | [harmonise/scorer.py](../harmonise/scorer.py) |
| Top-K orchestration + structural override | [harmonise/harmoniser.py](../harmonise/harmoniser.py) |
| Abbreviation dictionary (Layer 1 manual; Layer 2 mining + Layer 3 business loop scaffolded) | [harmonise/dictionary.py](../harmonise/dictionary.py) |
| Tests (22 cases) | [harmonise/tests/](../harmonise/tests/) |

---

## Verification

- **39 / 39 automated tests passing** ([api/tests/](../api/tests/) + [harmonise/tests/](../harmonise/tests/))
- Interactive API docs at `http://localhost:8000/docs` (Swagger UI) and `/redoc`
- OpenAPI 3.x spec exported to [submission/api_openapi.json](api_openapi.json)
