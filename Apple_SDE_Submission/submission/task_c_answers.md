# Task C — Technical Write-up

Three short, focused answers to the three Task C questions.

---

## C.1 — How does the data model adjust when new partner store data is inducted?

The Kimball star schema isolates partner-specific change to a **single dimension row** in most cases. The fact tables are designed to absorb new partners without DDL.

| Change | What needs to happen | Schema impact |
|--------|----------------------|---------------|
| **New partner, existing payment scheme** | `INSERT INTO dim_partner` (one row) | **None.** Existing `fact_price_offer` rows route to the new `partner_id` automatically. |
| **New payment type** (e.g. Buy Now Pay Later) | (1) `ALTER TYPE payment_type_enum ADD VALUE 'BNPL';` (2) `CREATE TABLE fact_payment_bnpl (...) PARTITION OF ...` (3) extend the `effective_total_local` computation in `services.submit_load_job` | One new child table; **existing rows untouched.** Class Table Inheritance is exactly what makes this clean. |
| **New country** | `INSERT INTO dim_country` + `INSERT INTO dim_timezone` (if a new IANA zone) + bootstrap `dim_currency_rate_snapshot` | **None on facts.** |
| **Unknown product category** in partner feed (e.g. a future Apple Vision SKU) | Detected implicitly via `DQ_HARM_002` (harmonise unmatched) — `dim_product_model` is seeded only from Product Reference, so an unknown category cannot match any model. Row lands in `dq_bad_records` with `raw_payload` intact. Business reviewer reads the raw name and decides: typo (RESOLVED + replay) vs genuinely new category → Apple catalog team adds it to Product Reference, re-seed `dim_product_model` + `dim_product_category`, replay batch. **Never auto-discovered.** | **None on facts.** Bad rows never reach `fact_price_offer`. |

**The takeaway.** Dimensions are designed to absorb business change; fact schemas stay stable for years. New partners onboard via configuration, not migrations — that is the star-schema's primary payoff.

---

## C.2 — Error handling & data-quality strategy involving business users

A three-tier loop. Detection is automated, triage is business-driven, and the system **learns** from each correction.

### Tier 1 — Automated detection (zero human in the loop)

- Every `POST /load-data` batch runs **13 Data Quality rules** in three stages:
  - **INGEST** (8 rules) on raw staging — nulls, format, range, payment-type conditional, country-code resolution. Failures never make it past staging.
  - **PRE_FACT** (3 HIGH-severity rules) on enriched staging — country↔currency, partner↔country, harmonise unmatched. **This is the gate**: rows failing here are flagged in `dq_bad_records` and **never enter `fact_price_offer`**, so analytics can trust the fact table by definition.
  - **SEMANTIC** (2 rules) on the freshly-written fact rows — low-confidence harmonise, category sanity bounds. These are single-row *soft signals* needing business judgment; failing rows STAY in fact and are flagged for triage. (Cross-row patterns — duplicates, temporal jumps, cross-partner variance — live in `/detect-anomalies`, not here.)
- Rule-level summary written to `dq_output` (one row per rule per batch run).
- Failing records written to `dq_bad_records` with `raw_payload` (JSONB) preserving the **full original CSV row** — even when types failed to parse, the evidence is intact for review.

### Tier 2 — Business-user triage (visual review interface)

A visualization interface lets non-technical business users review and resolve flagged records without writing code or filing engineering tickets. Concretely, the interface lets users:

- **Browse** all records currently in `dq_bad_records` filtered by `status` (`NEW` / `IN_REVIEW` / `RESOLVED` / `IGNORED`), `severity`, or `assignee`.
- **Inspect** each record side-by-side: the original `raw_payload` (full unmodified CSV row as JSON), the rule that failed (`rule_id`, `failed_field`), the human-readable error message, and the severity bucket — everything needed to understand the failure in one screen.
- **Act** on each record with two outcomes:
  - **RESOLVED + replay** — the dictionary or DQ rule was wrong; the user fixes it (e.g. adds an abbreviation, accepts an ISO code) and triggers surgical re-ingest of just that `source_batch_id` (not a full reload).
  - **IGNORED** — the data is genuinely bad / malformed; mark and move on.
- **Track** ownership and resolution history: `assignee`, `resolved_at`, `resolution_notes` are persisted on each row, giving a full audit trail without external tooling.

The backing API endpoints (`GET /bad-records` with filters + pagination, `POST /bad-records/{id}/resolve`) are implemented in this submission. A polished web front-end (e.g. a Kanban board where cards flow through status columns) is the natural next step but out of scope for this take-home — Swagger UI demonstrates the workflow today.

### Tier 3 — Learning loop

- Resolved records feed back into the system:
  - Harmonise dictionary additions (Layer 3 from the harmonise design).
  - Threshold tuning in `dim_anomaly_threshold`.
  - New DQ rules when a pattern emerges from bad-record reasoning.
- Quarterly review of `dq_output` trends (per-rule pass rate over time) flags rules that are noisy / drifting.

**Why this works.** Engineers don't know Apple product names as well as merchandising staff. Empowering business users to act without code deploys closes the gap between what the model knows and what the catalog actually contains. Every resolution makes the next batch better.

**Real example from the sample data.** 154 rows shipped with `COUNTRY_VAL = "NZ"` instead of `"New Zealand"`. The first DQ pass flagged them as `DQ_FMT_001` failures. Investigation showed both forms are valid representations of the same country — the resolver was simply too strict. The fix: extend `_COUNTRY_NAME_MAP` to accept ISO 3166-1 alpha-2 codes alongside full names. After the fix, those 154 rows entered `fact_price_offer` cleanly. **This is the C-2 loop in action.**

---

## C.3 — Scaling `POST /load-data` to 1 million records

Sync flow handles ~10 K rows in seconds; 1 M rows would take hours and HTTP would time out. Five changes solve it — three already done, two are the production gap.

| # | Change | Status |
|---|---|:---:|
| 1 | Async pipeline: HTTP 202 + S3 + `ingest_job` + chunked workers | Designed |
| 2 | PostgreSQL `COPY` instead of `INSERT` | Designed |
| 3 | Data Quality rules executed in SQL, not Python | ✅ **Implemented** |
| 4 | Reference data cached in-memory | ✅ **Partial** |
| 5 | Bulk Slowly Changing Dimension Type 2 update via a single CTE | ✅ **Implemented** |

---

### 1. Async pipeline — Designed

Replace the long-running HTTP request with: upload to AWS S3 → write `ingest_job` row → enqueue (SQS / Redis) → return **HTTP 202 + `job_id`** in seconds. A worker pool consumes the queue, splits each file into 10 K-row chunks, and processes chunks in parallel (multi-process, to bypass Python's GIL). Workers update `ingest_job.completed_chunks / rows_loaded / rows_bad` atomically; client polls `GET /load-data/{job_id}` for progress and ETA. Idempotency via `(source_batch_id, row_num)` UNIQUE; failed chunks retry up to 3× then go to a Dead Letter Queue.

### 2. `COPY` instead of `INSERT` — Designed

`COPY FROM STDIN` skips per-row SQL parsing — **50–100× faster** for bulk loads. One-line swap in [`_step1_csv_to_staging`](../api/services.py#L218): `executemany` → `copy_records_to_table`.

### 3. DQ rules in SQL — ✅ Implemented

13 rules are PL/pgSQL functions in [`dq/rules.sql`](../dq/rules.sql), called once per batch via `dq_run_batch_ingest` / `dq_run_batch_pre_fact` / `dq_run_batch_semantic`. Postgres vectorises each scan — **one DB call replaces 13 M Python checks** at 1 M rows.

### 4. Reference data in-memory — ✅ Partial

Done: `Harmoniser` registry (per-process singleton), country/currency maps (Python constants), `dim_partner` fetched once per batch. Gap: workers should also pre-load `dim_currency_rate_snapshot` at startup so chunk processing has zero DB round-trips.

### 5. Bulk SCD-2 — ✅ Implemented

[`_step6_scd2_history`](../api/services.py#L462) is already a single CTE (`latest → existing → changed → closed → insert`) — reconciles the whole batch in one query.

---

### Performance targets

| Volume | Current sync | Target |
|--------|---:|---:|
| 1 K | <1 sec | <1 sec |
| 1 M | 3–7 hours (times out) | **~90 sec** (~200×) |
| 10 M | not feasible | ~15 min |

**Migration path:** ✅ #3, #4, #5 already production-shaped → ① swap in `COPY` (1 day) → ② build async pipeline (`ingest_job` + queue + worker pool, the real lift).
