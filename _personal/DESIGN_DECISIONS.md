# Core Design Decisions / 核心设计决策

> **Purpose / 用途:** Single document of record for the 8 most important architectural choices in this project.
> Each entry describes **what was chosen, what was rejected, why, and what it costs us**.
>
> Use this as a Q&A cheat-sheet during the interview review session.
> 面试 Q&A 环节的 cheat sheet。

---

## 1. Class Table Inheritance for payment polymorphism / 支付方式用类表继承

**Decision / 决策:**
`fact_price_offer` is the parent table with `payment_type` as a discriminator
column. `fact_payment_full_price` and `fact_payment_instalment` are 1:1
child tables holding the type-specific columns.

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| Single wide table with NULLable columns | Assignment **explicitly forbids** sparse-table design |
| JSONB `payment_details` field | Loses type safety + CHECK constraints; harder to validate at schema level |
| Single Table Inheritance with type discriminator | Same sparse-column problem |

**Rationale / 理由:**
- ✅ Each child table has REAL `NOT NULL` and `CHECK` constraints
  (`full_price > 0`, `instalment_months BETWEEN 1 AND 60`).
- ✅ Adding a new payment type (e.g. BNPL — Buy Now Pay Later) means
  `ALTER TYPE payment_type_enum ADD VALUE 'BNPL'` + create a new child
  table. **Zero impact on existing rows.**
- ✅ Maps directly to OOP "polymorphism" — easy to explain.

**Cost / 代价:**
- A 1:1 join is needed to get full payment detail.
- ETL must write to two tables in the same transaction.

**Where it lives / 位置:**
- `schema.sql` `fact_price_offer` + `fact_payment_full_price` + `fact_payment_instalment`
- `submission/task_a_schema.sql` (same)
- Rationale: `design_notes.md` A.1 entry #3.

---

## 2. Bi-temporal facts (business time + system time) / 双时间事实表

**Decision / 决策:**
Every fact row has both `crawl_ts_utc` (when the price was observed at the
partner's site, i.e. business time) and `ingested_at` (when our system
loaded it, i.e. system time).

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| Only `crawl_ts_utc` | Can't distinguish "late-arriving data" from "real-time data"; replay debugging blind |
| Only `ingested_at` | Loses business timeline; can't answer "what did the price look like on date X" |

**Rationale / 理由:**
- ✅ Late arrivals (e.g. Partner sends a corrected feed for last week)
  are correctly time-stamped at business time but ordered by ingestion.
- ✅ System / pipeline issues (slow workers, retries) traceable via
  `ingested_at`.
- ✅ Replay safe — we can re-ingest historical data with new logic and
  preserve the original observation time.

**Cost / 代价:**
- One extra column on every fact row. Storage cost is trivial relative
  to the analytical value.

**Where it lives / 位置:**
- `schema.sql` `fact_price_offer.crawl_ts_utc` + `ingested_at`
- `design_notes.md` A.1 entry #4.

---

## 3. Currency frozen as a snapshot inside the fact row / 汇率冷冻进事实行

**Decision / 决策:**
Each fact row stores: `currency_code`, `effective_total_local`,
`effective_total_usd`, `fx_rate_to_usd`, `fx_rate_date`. The FX rate
is **looked up once at load time** and frozen alongside the row.

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| Compute USD on the fly via JOIN to `dim_currency_rate_snapshot` | Slow (every analytical query needs the JOIN); breaks if a historical rate is corrected |
| Store local only; treat USD as a derived view | Same query-time JOIN problem |

**Rationale / 理由:**
- ✅ **O(1) analytical queries** — anomaly detection / dashboards read
  USD directly without a rate-table JOIN.
- ✅ **Audit trail** — we can answer "what rate did we use on this row?"
  exactly.
- ✅ **Immutability** — if a future ETL job re-publishes corrected FX
  rates, historical fact rows keep their original USD value.
- ✅ **Recomputability** — `currency_code + effective_total_local +
  fx_rate_date` together give us enough to recompute USD if needed.

**Cost / 代价:**
- Slight column-count increase on the fact row.
- Manual FX rate corrections need explicit re-ingestion if we want
  historical values to change.

**Where it lives / 位置:**
- `schema.sql` `fact_price_offer` (currency block).
- `design_notes.md` A.1 entry #6.

---

## 4. SCD Type 2 history table for compressed price tracking / SCD-2 价格历史

**Decision / 决策:**
`fact_partner_price_history` stores **only meaningful price changes**
using the Slowly Changing Dimension Type 2 pattern: each row has
`valid_from_date` and `valid_to_date`; current price has
`valid_to_date = NULL`.

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| Use `fact_price_offer` directly for as-of queries | Massive redundancy — partners change prices ~weekly, we'd store ~140× more rows than needed |
| Type 1 (overwrite) | Loses all history — needed for trend analysis & anomaly baselines |
| Type 3 (previous-value column) | Only one step of history, insufficient for rolling baselines |

**Rationale / 理由:**
- ✅ **20-50× storage reduction** vs. raw event stream.
- ✅ **As-of queries are O(log n)**: `WHERE valid_from_date <= D AND
  COALESCE(valid_to_date, '9999-12-31') >= D`.
- ✅ Powers `v_partner_price_current` (live snapshot view) and
  `mv_baseline_staging` (rolling stats).
- ✅ Anomaly detection's "previous price" lookup is now a single index
  hit instead of an aggregation.

**Cost / 代价:**
- ETL must do a change-detection step (compare new observation to
  current row).
- Schema is harder for newcomers to understand than a simple events table.

**Where it lives / 位置:**
- `schema.sql` `fact_partner_price_history`
- `design_notes.md` A.5 + A.6.

---

## 5. A+MV hybrid for anomaly baselines / A+MV 混合基线

**Decision / 决策:**
`mv_baseline_staging` (a MATERIALIZED VIEW) holds the *calculation
logic* in plain SQL. A scheduled job INSERTs from the MV into the
physical table `dws_product_price_baseline_1d` with **write-time dedup**
(skip rows identical to yesterday's).

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| Only a MATERIALIZED VIEW | Each `REFRESH` overwrites yesterday's snapshot — no history; can't dedup |
| Only a physical table populated by Python ETL | Calculation logic lives in code, not in SQL; harder to review/test |
| Compute baselines on-demand at API time | Too slow at Apple-scale (thousands of products × 3 windows × per-call) |

**Rationale / 理由:**
- ✅ **Calculation logic is reviewable SQL** — analysts can audit it
  without reading Python.
- ✅ **History preserved** — yesterday's baseline still in the physical
  table; tomorrow's only added if it differs.
- ✅ **Storage efficient** — for stable products, the baseline row is
  written once and re-used for many days.
- ✅ **API queries are O(1)** — anomaly detection just does a lookup.

**Cost / 代价:**
- Two artifacts to maintain (MV + table) instead of one.
- Slightly more complex deployment (initial MV refresh + table seeding).

**Where it lives / 位置:**
- `schema.sql` `dws_product_price_baseline_1d` + `mv_baseline_staging`
- `design_notes.md` A.6.

---

## 6. Per-signal anomaly rows / 每信号一行异常记录

**Decision / 决策:**
`fact_anomaly` stores **one row per triggered signal**, not one row per
anomalous offer. If an offer trips three signals (STATISTICAL,
TEMPORAL, CROSS_PARTNER), three independent rows are emitted, each with
its own severity and routing.

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| One row per offer with combined severity | Strong individual signals get diluted by averaging; can't route "this is a temporal anomaly" to operations vs. "cross-partner anomaly" to strategy |
| Bitmask of triggered signals in a single row | Hard to attach per-signal status / assignee / resolution notes |
| Three separate fact tables (one per signal type) | Triples DDL maintenance; no shared queries |

**Rationale / 理由:**
- ✅ Each concern can be **routed to the right team** (operations vs
  pricing strategy).
- ✅ Each concern has its own **resolution workflow** (status,
  assignee, notes).
- ✅ Signal-level reporting becomes a simple `GROUP BY anomaly_type`.
- ✅ Adding a new signal type doesn't change the schema.

**Cost / 代价:**
- An offer with N signals creates N rows — slightly more storage.
- "Total unique anomalous offers" requires `COUNT(DISTINCT offer_id)`.

**Where it lives / 位置:**
- `schema.sql` `fact_anomaly`
- `design_notes.md` B.4.

---

## 7. All thresholds in `dim_anomaly_threshold` config / 阈值集中配置

**Decision / 决策:**
Every magic number used by the anomaly detector — signal weights,
severity bars, lifecycle factors, event-suppression factors, category
volatility multipliers — lives as a row in `dim_anomaly_threshold`,
**not** in code.

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| Hard-coded constants in Python | Business iteration requires code deploy; values are opaque to non-engineers |
| Environment variables | Doesn't support per-category / per-country overrides |
| YAML config file | No audit trail (`updated_by`, `last_reviewed_at`); no DB-level integrity |

**Rationale / 理由:**
- ✅ **Business teams can iterate without code changes.**
- ✅ Per-category and per-country **overrides** supported via
  `(config_key, category_code, country_code)` UNIQUE.
- ✅ `rationale` and `source` columns force **assumption documentation**
  ("initial_guess" vs "data_calibrated" vs "business_agreed").
- ✅ Calibration plans in `design_notes.md` Appendix Cal.1–7 explain
  how each placeholder gets replaced over time.
- ✅ `threshold_snapshot JSONB` in `fact_anomaly` freezes the exact
  config used at detection time → **replay-safe even if the config
  changes later**.

**Cost / 代价:**
- One DB lookup per anomaly check (mitigated by short cache).
- Slightly more friction to bootstrap a new deployment (must seed the
  config table).

**Where it lives / 位置:**
- `schema.sql` `dim_anomaly_threshold` + `get_anomaly_threshold()` function
- `design_notes.md` Appendix Cal.1–Cal.7 (calibration methodologies)

---

## 8. Lean indexing strategy / 精益索引策略

**Decision / 决策:**
**No FK indexes on small dimension tables**. Multiple **targeted
composite indexes on large fact tables**. Final count: 14 explicit
indexes (down from an initial draft of 20+).

**Alternatives considered / 备选方案:**
| Option | Why rejected |
|--------|--------------|
| Index every FK column "just in case" | Storage + maintenance overhead with zero query benefit on small tables |
| Skip indexing entirely; rely on PK auto-index | Fact tables at Apple scale (~30M rows/year) NEED composite indexes for analytical queries |

**Rationale / 理由:**
- ✅ Postgres auto-indexes PK and UNIQUE constraints — no need to repeat.
- ✅ Postgres does **NOT** auto-index FK columns — those need explicit
  indexes IF the table is big enough to benefit.
- ✅ Small dim tables (~1k–10k rows) get sub-millisecond table scans;
  indexes would cost more than they save.
- ✅ Each remaining index is justified by a specific high-frequency
  query pattern (and each one in `schema.sql` has an inline `-- DESIGN:`
  comment explaining which query it serves).

**Cost / 代价:**
- Initial loading without indexes is faster; adding them later is fine.
- Some safety net is lost — adding a new query pattern might require
  a new index.

**Where it lives / 位置:**
- `schema.sql` (each `CREATE INDEX` has a `-- DESIGN:` comment)
- Skipped indexes are documented inline (e.g. "no FK index here because
  the table is < 10k rows; revisit if EXPLAIN ANALYZE shows a problem").
- `presentation.md` Slide 15 ("Index sizing — less is more")

---

## 9. Orchestrator endpoint + 4 Task-B sub-modules sharing 9 helpers / 编排端点 + 4 个子模块共用 9 helper

| English | 中文 |
|---|---|
| **Decision.** Implement the API as one independent FastAPI service with **two call paths sharing the same 9 internal step helpers**: (a) `POST /pipeline` orchestrator that runs all 9 steps in a single PostgreSQL transaction with the PRE_FACT hard gate enabled, and (b) the 4 Task-B literal sub-modules (`/load-data`, `/compute-dq`, `/detect-anomalies`, `/harmonise-product`) each independently callable and covering a coherent group of steps. Both paths invoke the same Python helper functions in `api/services.py` — zero code duplication. | **决策。** API 作为一个独立 FastAPI 服务,**两条调用路径共享同一组 9 个内部 step helper**:(a) `POST /pipeline` 编排端点单事务跑完 9 步,带 PRE_FACT 硬 gate;(b) Task B 字面要求的 4 个子模块 (`/load-data` / `/compute-dq` / `/detect-anomalies` / `/harmonise-product`) 各自独立可调,每个覆盖一组连贯的步骤。两条路径调用 `api/services.py` 里**同一组** Python helper —— 零代码重复。 |
| **Why.** Task B's literal contract requires 4 independently callable endpoints — that's Path B. But hard PRE_FACT gating is **only possible inside a single-process sequential execution** because `/load-data` must be independently callable (it cannot wait for `/compute-dq` to decide whether to write fact). The orchestrator endpoint recovers the gating semantic without violating Task B's modular requirement. The trade-off is documented and demonstrable: same dataset on Path A → `fact_price_offer` is clean; on Path B → fact contains flagged rows that analytics queries must filter via `LEFT JOIN dq_bad_records`. | **为什么。** Task B 字面要求 4 个独立可调端点 —— 这给出 Path B。但**硬 PRE_FACT gate 只能在单进程顺序执行里实现**,因为 `/load-data` 必须独立可调(不能等 `/compute-dq` 决定是否写 fact)。Orchestrator 端点恢复了 gate 语义,同时不违反 Task B 的模块化要求。Trade-off 可演示:同一份数据走 Path A → `fact_price_offer` 干净;走 Path B → fact 含标记行,分析查询要 `LEFT JOIN dq_bad_records` 过滤。 |
| **What it costs.** A second top-level endpoint to maintain alongside the 4 Task-B sub-modules. Two distinct semantic contracts (single-transaction vs three-transaction) that must be tested independently; we have path-parity tests that verify both cover all 9 steps with the gating difference observable. | **代价。** 在 4 个 Task-B 子模块之外多维护一个顶层端点。两套不同的语义契约(单事务 vs 三事务)必须独立测试;我们有 path-parity 测试验证两条路径都覆盖 9 步且 gate 差异可观测。 |
| **Rejected options.** (1) **Only `/pipeline`, drop sub-modules** — fails Task B's literal specification. (2) **Only sub-modules, no orchestrator** — gives up hard gating; bad rows always enter fact. (3) **Sub-modules call `/compute-dq` synchronously inside `/load-data`** — defeats the "independently callable" requirement and the original 8-step pipeline collapses back into a mega-endpoint. (4) **Add a 5th endpoint that triggers fact write after DQ** — exceeds Task B's 4-endpoint scope and fragments the user-facing flow. | **否决方案。** (1) **只 `/pipeline`,砍掉子模块** —— 不满足 Task B 字面要求。(2) **只子模块,不要 orchestrator** —— 放弃硬 gate;坏行总会进 fact。(3) **`/load-data` 内部同步调 `/compute-dq`** —— 违反"独立可调"要求,8 步流水线又退化成大杂烩。(4) **加第 5 个端点专门做 DQ 后的 fact promote** —— 超出 Task B 4 个端点的范围,用户路径碎片化。 |
| **Where to read more.** | **延伸阅读。** |
| - `api/services.py` — 9 helpers + entry points (`run_pipeline`, `submit_load_job`, `compute_dq_service`, `detect_anomalies_service`) | - `api/services.py` —— 9 helper + 入口函数 |
| - `api/main.py` — 5 endpoints + dual-path docstrings | - `api/main.py` —— 5 个端点 + 双路径 docstring |
| - `submission/task_b_answers.md` — 9-step mapping table + Path A vs Path B comparison | - `submission/task_b_answers.md` —— 9 步映射表 + Path A vs B 对比 |
| - `presentation.md` Slide 5–6 + Slide 12 — architecture + Path A vs B trade-off | - `presentation.md` Slide 5–6 + Slide 12 —— 架构 + Path A vs B 取舍 |

---

## 📝 Maintenance Note / 维护备忘

When a new significant architectural decision is made, add it to this
list. When an existing decision is reversed, **mark it deprecated** but
keep the entry — the rejected-options column may save someone from
re-debating later.

新增重大架构决策时**追加到本列表**。如果某个决策被反转，**标 deprecated 但保留条目** —— "为什么否决"那一列将来可能避免重复讨论同一个问题。
