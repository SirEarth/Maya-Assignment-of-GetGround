# Design Notes — Maya Assignment of GetGround / 设计说明

> Running log of design decisions, rationale, and calibration plans for the final report + presentation.
> 面试报告与演示用的设计决策、原因、校准方案的滚动记录。
>
> Bilingual format: each section has English first, then Chinese (中文) parallel.
> 双语格式：每节先英文，后中文。

---

## 0. Business Context / 业务背景

### EN
Scrape, harmonise, and analyse product pricing offers from multiple partner stores (Apple retailers). System must:
- Reconcile multi-source pricing differences (different schemas per partner)
- Support multi-payment models (full price vs instalment) without sparse columns
- Handle multi-currency, multi-timezone analytics
- Monitor data quality with business-user correction workflow
- Detect pricing anomalies with severity classification + visualization

**Sample data:** Partner A (Australia, instalment model) + Partner B (New Zealand, full price model) + Product Ref lookup table.

### 中文
从多个合作商店（Apple 零售商）爬取、标准化并分析商品定价报价。系统需要：
- 调和多数据源的定价差异（每个 Partner 有不同的 schema）
- 支持多种支付模式（全款 vs 分期），且**不能使用稀疏表**
- 处理多货币、多时区分析
- 监控数据质量并提供**业务人员参与**的纠错工作流
- 检测定价异常，提供严重度分级 + 可视化

**样本数据：** Partner A（澳洲，分期模式）+ Partner B（新西兰，全款模式）+ Product Ref 查找表。

---

## A. Data Model Design / 数据模型设计

### A.1 Core Design Principles / 核心设计原则

#### EN

| # | Principle | Why |
|---|-----------|-----|
| 1 | **Kimball star schema** | Clear fact/dim separation; dim changes don't churn fact tables; fact growth doesn't bloat dim maintenance |
| 2 | **Immutable event facts** | Each crawl = new row, never UPDATE. Corrections = new rows with later `ingested_at`. Enables replay and audit |
| 3 | **Class Table Inheritance for payment types** | `fact_price_offer` is parent with `payment_type` discriminator; `fact_payment_full_price` / `fact_payment_instalment` are 1:1 children. **Zero sparse columns**, which the assignment explicitly forbids |
| 4 | **Bi-temporal design** | `crawl_ts_utc` = business time (when observed on partner site); `ingested_at` = system time (when loaded). Distinction matters for late arrival and debugging |
| 5 | **Timezone-aware** | `crawl_ts_utc` + derived `crawl_ts_local` + `crawl_date_local` (generated stored). Enables both global UTC analytics and regional market-local reporting |
| 6 | **Currency denormalization (frozen snapshot)** | Store `effective_total_local` + `effective_total_usd` + `fx_rate_to_usd` + `fx_rate_date` directly in fact row. O(1) analytical queries, full audit trail, immutability, recomputability |
| 7 | **Monthly partitioning** | `fact_price_offer PARTITION BY RANGE (crawl_ts_utc)`. Month granularity balances query pruning with partition count overhead. Aligns with Apple fiscal months |
| 8 | **Auto-partition via pg_partman** | Production pattern; 3 demo months hardcoded for schema.sql runnability. `_default` partition catches out-of-range inserts |

#### 中文

| # | 原则 | 理由 |
|---|-----|-----|
| 1 | **Kimball 星型模型** | Fact/Dim 清晰分离；维度变化不影响事实表；事实表增长不拖累维度维护 |
| 2 | **不可变事件事实表** | 每次爬取 = 新行，永不 UPDATE。修正 = 写新行并附更晚的 `ingested_at`。支持回放和审计 |
| 3 | **支付类型用类表继承（CTI）** | `fact_price_offer` 为父表 + `payment_type` 判别列；`fact_payment_full_price` / `fact_payment_instalment` 为 1:1 子表。**零稀疏列**，作业硬性要求 |
| 4 | **Bi-temporal 双时间设计** | `crawl_ts_utc` = 业务时间（对方网站观测时刻）；`ingested_at` = 系统时间（入库时刻）。区分两者对延迟到达和调试至关重要 |
| 5 | **时区感知** | `crawl_ts_utc` + 派生的 `crawl_ts_local` + `crawl_date_local`（GENERATED STORED）。同时支持全球 UTC 分析和区域本地报表 |
| 6 | **货币去规范化（冷冻快照）** | 在事实表行里直接存 `effective_total_local` + `effective_total_usd` + `fx_rate_to_usd` + `fx_rate_date`。O(1) 查询、完整审计、不可变性、可重算 |
| 7 | **按月分区** | `fact_price_offer PARTITION BY RANGE (crawl_ts_utc)`。月粒度在剪枝和分区数开销间取得平衡，还天然对齐 Apple 财月 |
| 8 | **pg_partman 自动分区** | 生产标准实践；schema.sql 硬编码 3 个月作为演示可运行。`_default` 分区兜底超出范围的写入 |

### A.2 Naming Convention / 命名规范

| Prefix | Meaning | 含义 |
|--------|---------|------|
| `dim_` | Dimension tables | 维度表（描述性） |
| `fact_` | Fact tables (events, measurements) | 事实表（事件、度量）|
| `dws_` | Data Warehouse Summary (aggregates) | 数据仓库汇总层（period suffix: `_1d` / `_1h` / `_td`）|
| `dq_` | Data quality tracking | 数据质量追踪 |
| `app_` | Application-layer configuration | 应用层配置 |
| `v_` | Views | 视图 |
| `mv_` | Materialized views | 物化视图 |

All snake_case. Surrogate keys: `<entity>_id`. Natural keys: `<entity>_code`. Timestamps: `_ts` / `_at` / `_date`.
统一 snake_case。代理键 `<entity>_id`；业务键 `<entity>_code`；时间字段 `_ts`（时刻）/ `_at`（动作）/ `_date`（日期）。

### A.3 Product Harmonisation — Model vs SKU / 产品标准化粒度

#### EN
**Decision:** match at `product_model_id` (merged) level, not SKU (color) level.

**Reasoning:**
- Partner data rarely contains color info (Partner A has none; Partner B has it inconsistently)
- Apple typically prices all colors identically — merging increases statistical sample size 4-5×
- Cross-partner anomaly detection works better with aggregated samples

**SKU info preserved in `dim_product_sku`** for traceability (`ref_product_id` links back to Product Ref.csv). Added `sku_id` to fact tables for cases where partner data distinguishes SKUs — enables a separate `SKU_VARIANCE` anomaly type.

#### 中文
**决策：** 在 `product_model_id`（合并型号）级别匹配，不到 SKU（颜色）级。

**理由：**
- Partner 数据极少含颜色（Partner A 没有；Partner B 不稳定）
- Apple 不同颜色基本同价 —— 合并后统计样本量翻 4-5 倍
- 跨 Partner 异常检测在聚合样本下更稳健

**SKU 信息保留在 `dim_product_sku`** 用于追溯（`ref_product_id` 回指 Product Ref.csv）。事实表加了 `sku_id` 可空字段，供 Partner 数据区分 SKU 时使用 —— 支撑独立的 `SKU_VARIANCE` 异常类型。

### A.4 Timezone + Multi-Stakeholder Design / 时区与多角色设计

#### EN
**Problem:** "today" means different dates for different stakeholders
- Regional Sales (AU) → Sydney local day
- Finance → Cupertino / Pacific Time (Apple fiscal)
- Strategy (HQ in London) → UTC or London time

**Solution:**
- `dim_timezone` stores IANA names (never manual offsets — DST-safe)
- `dim_country.primary_tz_id` links country → main commercial timezone
- `fact_price_offer` stores BOTH `crawl_ts_utc` and derived `crawl_ts_local` / `crawl_date_local`
- `dim_user_preference` holds per-user routing (tz, role, currency, fiscal calendar)
- API routes to appropriate aggregate based on role

#### 中文
**问题：** "今天"对不同角色的含义不同
- 区域销售（AU）→ Sydney 本地日
- Finance → Cupertino / Pacific Time（Apple 财年）
- 战略团队（伦敦 HQ）→ UTC 或伦敦时间

**方案：**
- `dim_timezone` 存 IANA 时区名（绝不手工存 offset —— DST 安全）
- `dim_country.primary_tz_id` 把国家关联到主商业时区
- `fact_price_offer` 同时存 `crawl_ts_utc` 和派生的 `crawl_ts_local` / `crawl_date_local`
- `dim_user_preference` 保存用户级路由（时区/角色/货币/财历）
- API 按角色分流到对应聚合表

### A.5 Aggregate Layer Evolution / 聚合层演化

#### EN
Initial design had 7 aggregate tables. Trimmed to 4 + 1 VIEW + 1 MV.

| Removed | Reason |
|---------|--------|
| `dws_price_offer_market_local_1d` | No API endpoint depends on it; fact table + `(country, date_local)` index serves same queries |
| `dws_price_offer_td` | Portfolio-overview is "nice to have", not in assignment; computable on-demand |
| `dws_cross_partner_comparison_1d` | Replaced with `v_partner_price_current` VIEW. Prices rarely change → daily aggregate = 90% duplicate rows |
| `dws_partner_dq_1d` | Replaced with `dws_partner_dq_per_batch` (event-driven). Partner load is irregular; per-batch grain captures each load as immutable event |

**Kept + added:**
- `dws_product_price_baseline_1d` — rolling stats with `mv_baseline_staging` MV staging layer
- `fact_partner_price_history` — SCD Type 2 compressed (20-50× reduction)
- `dws_partner_dq_per_batch` — event-driven DQ summary
- `fact_anomaly` — per-signal anomaly audit record

#### 中文
初期设计 7 张汇总表，精简到 4 张 + 1 VIEW + 1 MV。

| 删除对象 | 原因 |
|---------|------|
| `dws_price_offer_market_local_1d` | 无 API 端点依赖；事实表 + `(country, date_local)` 索引能服务相同查询 |
| `dws_price_offer_td` | 产品组合概览属"锦上添花"非必需；按需现算即可 |
| `dws_cross_partner_comparison_1d` | 换成 `v_partner_price_current` VIEW。价格变动稀疏 → 日聚合 90% 是重复行 |
| `dws_partner_dq_1d` | 换成 `dws_partner_dq_per_batch`（event-driven）。Partner 加载不定期；按批次粒度把每次 load 当作不可变事件记录 |

**保留 + 新增：**
- `dws_product_price_baseline_1d` —— 滚动统计 + `mv_baseline_staging` MV 计算中间层
- `fact_partner_price_history` —— SCD Type 2 压缩（20-50 倍压缩比）
- `dws_partner_dq_per_batch` —— event-driven DQ 汇总
- `fact_anomaly` —— 每信号一行的异常审计表

### A.6 Key Design Pattern: A+MV Hybrid for Baselines / A+MV 混合模式

```
fact_partner_price_history (SCD-2 compressed / SCD-2 压缩历史)
        ↓  MATERIALIZED VIEW
mv_baseline_staging (daily computation / 每日计算)
        ↓  INSERT + write-time dedup / 写入端去重
dws_product_price_baseline_1d (physical, audit-ready / 物理表，可审计)
        ↓  O(1) lookup / 毫秒查询
POST /detect-anomalies
```

**Benefits / 好处:**
- Calculation logic lives in SQL (reviewable, testable) / 计算逻辑写在 SQL（可审阅、可测试）
- Audit trail preserved (yesterday's baseline not overwritten) / 保留审计追溯（昨天的基线不被覆盖）
- Write-time dedup avoids storage waste / 写入端去重避免存储浪费
- API serves from physical table = millisecond response / API 从物理表读，毫秒级响应

---

## B. API Design / API 设计

### B.1 GET /harmonise-product — Product Name Harmonisation / 商品名标准化

#### EN

**Hybrid algorithm:** structured attribute extraction + fuzzy fallback.

**Pipeline:**
1. **Normalize** — lowercase, strip noise (`apple`, `storage`, `-inch`), expand abbreviations from dictionary (`iP→iphone`, `PM→pro max`, `TB→1024gb`)
2. **Attribute extraction** — parse `(category, model_line, chip, storage_gb, connectivity)` tuple
3. **Multi-signal scoring:**
   ```
   score = 0.5 × attr_match      # structured overlap (dominant)
         + 0.3 × token_jaccard    # cleaned token set Jaccard
         + 0.2 × char_fuzz_ratio  # character-level backup
   ```
4. **Top-K ranking** with confidence buckets

**Abbreviation dictionary strategy (3-layer):**
- **Layer 1: Manual core** (20-30 entries) — domain knowledge, can't be auto-mined
- **Layer 2: Data-driven mining** — TF-IDF + N-gram co-occurrence from Product Ref.csv Long→Short Description mapping, extract candidates
- **Layer 3: Business loop** — Low-confidence matches land in `dq_bad_records`, business users review and promote tokens to dictionary (closes Task C-2 loop)

**Why NOT sentence-transformers:**
- Domain abbreviations (`iP`, `PM`, `A16`) aren't in pretrained vocab
- 281 Product Ref rows — structured matching already excellent
- Explanation matters for DQ review; vector similarity is opaque
- Kept as pluggable `score_fn="embedding"` option for future scale

**Confidence tiers:** HIGH (≥0.85) / MEDIUM (0.60-0.85) / LOW (<0.60 → auto bad_records) / MANUAL.

**Structural override rule (crucial for avoiding human-review overload):**
When `attr_match >= 0.95`, confidence is UPGRADED to HIGH regardless of combined score. Rationale: if the core identifying attributes (category + storage + model_line) align perfectly, partner-side verbosity or missing suffixes (e.g. `iP15P 128` omits "GB") should not force business review. The structural signal is dispositive. Without this rule, queries like `iP15P 128` landed at 0.695 MEDIUM and would queue for manual review — unsustainable volume at scale.

**Storage inference fallback:**
When a partner omits the "GB" suffix, check standalone digit tokens against Apple SKU storage sizes `{64, 128, 256, 512, 1024, 2048}`. This set is intentionally narrow to avoid false positives — iPad screen sizes (11/13) and iPhone model years (15/16/17) are NOT in the set.

#### 中文

**混合算法：** 结构化属性抽取 + 模糊匹配兜底。

**流水线：**
1. **标准化** —— 转小写、去噪声（`apple`、`storage`、`-inch`）、按字典展开缩写（`iP→iphone`、`PM→pro max`、`TB→1024gb`）
2. **属性抽取** —— 解析成 `(category, model_line, chip, storage_gb, connectivity)` 元组
3. **多信号评分：**
   ```
   score = 0.5 × attr_match      # 结构化属性重合度（主导）
         + 0.3 × token_jaccard    # 清洗后的 token 集合 Jaccard
         + 0.2 × char_fuzz_ratio  # 字符级模糊兜底
   ```
4. **Top-K 返回** + 置信度分档

**缩写字典策略（三层）：**
- **Layer 1：手工核心**（20-30 条）—— 领域知识，自动挖不出来
- **Layer 2：数据驱动挖掘** —— 从 Product Ref.csv Long Description → Short Description 用 TF-IDF + N-gram 共现挖候选
- **Layer 3：业务闭环** —— 低置信匹配落到 `dq_bad_records`，业务人员审核后把 token 升级进字典（对接 Task C-2 闭环）

**为什么不用 sentence-transformers：**
- 领域缩写（`iP`、`PM`、`A16`）不在预训练语料里
- Product Ref 只有 281 行，结构化匹配已经足够
- DQ 审核需要可解释性，向量相似度是黑盒
- 保留 `score_fn="embedding"` 可插拔接口，为未来扩展留口子

**置信度分档：** HIGH（≥0.85）/ MEDIUM（0.60-0.85）/ LOW（<0.60，自动进 bad_records）/ MANUAL。

**结构化 override 规则（避免人工审核过载的关键）：**
当 `attr_match >= 0.95` 时，置信度**直接升级为 HIGH**，不看综合分。理由：如果核心识别属性（category + storage + model_line）完美对齐，Partner 的冗长命名或缺省后缀（如 `iP15P 128` 省略了 "GB"）不应该拖累系统要求人工审核。结构化信号已经足以下判断。没有这条规则前，像 `iP15P 128` 这种只给 0.695 MEDIUM，在规模化时业务审核队列会被淹没。

**存储容量兜底识别：**
当 Partner 省略 "GB" 后缀时，检查独立数字 token 是否落在 Apple SKU 存储容量集合 `{64, 128, 256, 512, 1024, 2048}` 中。这个集合故意窄 —— iPad 屏幕尺寸（11/13）和 iPhone 型号年份（15/16/17）**不在**集合里，避免误识别。

### B.2 POST /pipeline + POST /load-data — Two-Path Ingestion Architecture / 双路径数据加载架构

#### EN

**Architectural decision.** The API exposes **two call paths sharing the same 9 internal step helpers** in `api/services.py`:

- **Path A — `POST /pipeline`** (orchestrator) — runs all 9 steps in a single PostgreSQL transaction with PRE_FACT hard gate enabled
- **Path B — 4 Task-B sub-modules** (`/load-data` + `/compute-dq` + `/detect-anomalies` + `/harmonise-product`) — each independently callable; covers the same 9 steps in grouped transactions; PRE_FACT degrades to post-hoc flagging

Both paths invoke the same Python helpers — zero code duplication.

**Why two paths.** Task B requires 4 independently callable endpoints (Path B). But hard PRE_FACT gating is **only possible inside single-process sequential execution** because `/load-data` cannot wait for `/compute-dq` to decide whether to write fact. The orchestrator endpoint recovers the gating semantic without violating Task B's modular requirement.

**Key principle for fact_price_offer.** It's an *event* table — only stores **meaningful price events** (new product, or price change vs current `fact_partner_price_history` row). Crawl observations that show the same price as before are NOT inserted there; only the per-batch counter `dws_partner_dq_per_batch.rows_unchanged` is incremented. Every row represents a business event, not a redundant scrape result.

**The 9 steps (same helpers, different orchestration order):**

1. **`parse_csv_to_stg`** — parse record/file → INSERT into `stg_price_offer` (raw payload preserved for audit regardless of validity)
2. **`run_ingest_dq`** — INGEST-stage DQ (8 rules: nulls, format, range, conditional) on raw stg → populate `dq_output` + `dq_bad_records` for failures. Mark passing rows with `dq_status='INGEST_PASSED'`.
3. **`harmonise_in_stg`** — call the harmoniser (in-process) for each row → write `product_model_id`, `harmonise_score`, `harmonise_confidence` back to stg.
4. **`run_prefact_dq`** — PRE_FACT-stage DQ (3 HIGH-severity rules: country↔currency match, partner↔country match, harmonise unmatched) on the enriched stg. Failing rows are recorded in `dq_bad_records`. Mark survivors with `dq_status='PRE_FACT_PASSED'`.
5. **`write_stg_to_fact(gate=…)`** — **the divergence point**:
   - **Path A (`gate=True`)**: only PRE_FACT_PASSED rows enter `fact_price_offer`. Bad rows blocked at the boundary. `fact_price_offer` is trustworthy by construction; downstream analytics trust the table without needing filter-views.
   - **Path B (`gate=False`)**: all parseable rows enter fact. Bad rows are flagged in `dq_bad_records` post-hoc by step 2/4 (which run later via `/compute-dq`). Analytics queries must `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL` to filter.
   - Both paths build the row from: country → ISO code (via `dim_country`); timestamp → UTC + market-local; FX rate frozen from `dim_currency_rate_snapshot` using `crawl_date`; `effective_total_local` + `effective_total_usd` computed; INSERT into `fact_price_offer` + child payment table (`fact_payment_full_price` or `fact_payment_instalment` per CTI design).
6. **`run_semantic_dq`** — SEMANTIC-stage DQ (2 rules: low-confidence harmonise + category sanity bounds) on the freshly-written fact rows. **Failing rows STAY in fact** — these are *soft signals* where business judgment is required. Records flagged in `dq_bad_records` for review via `/bad-records`. Cross-row pricing patterns (variance, temporal jumps, cross-partner divergence) live in `/detect-anomalies`, not in DQ.
7. **`update_scd2`** — SCD-2 history reconciliation (single CTE: latest → existing → changed → closed → insert) updates `fact_partner_price_history`.
8. **`detect_anomalies_for_batch`** — STATISTICAL signal vs 30-day baseline from SCD-2 history; build visualization payload (series + band + cross-partner). Path A runs this inline; Path B runs it via `/detect-anomalies`.
9. **`write_batch_summary`** — UPSERT into `dws_partner_dq_per_batch` (loaded_records, bad_records_count, harmonise stats). Idempotent; on Path B each endpoint refreshes the row at its tail so the summary fills incrementally.

**Path A invocation order:** 1→2→3→4→5(gate=True)→6→7→8→9 inside one transaction.
**Path B invocation order:** /load-data does {1, 3, 5(gate=False), 7}; /compute-dq does {2, 4, 6}; /detect-anomalies does {8}; each endpoint runs step 9 at its tail.

**Severity policy.** HIGH severity = blocks from fact on Path A (PRE_FACT stage); flagged on Path B. MEDIUM/LOW severity = stays in fact, flagged for triage (SEMANTIC stage). Adding a new gate rule = one row in `dq_rule_catalog` with `target_stage='PRE_FACT'`, no code change.

#### 中文

**架构决策。** API 暴露**两条共用同一组 9 个内部 step helper 的调用路径**(都在 `api/services.py`):

- **Path A — `POST /pipeline`**(编排端点)—— 单 PostgreSQL 事务里跑完 9 步,带 PRE_FACT 硬 gate
- **Path B — Task B 4 个子模块**(`/load-data` + `/compute-dq` + `/detect-anomalies` + `/harmonise-product`)—— 各自独立可调,分组多事务覆盖同样 9 步;PRE_FACT 退化为事后标记

两条路径调用**同一组** Python helper —— 零代码重复。

**为什么两条路径。** Task B 字面要求 4 个独立可调端点(Path B)。但**硬 PRE_FACT gate 只能在单进程顺序执行里实现**——`/load-data` 不能等 `/compute-dq` 决定是否写 fact。Orchestrator 端点恢复了 gate 语义,同时不违反 Task B 的模块化要求。

**fact_price_offer 的核心原则。** 是**事件表** —— 只存储**有意义的价格事件**(新产品、或相对 `fact_partner_price_history` 当前行有价格变化)。和上次价格相同的爬取观测**不**写入 `fact_price_offer`;只把每批的 `dws_partner_dq_per_batch.rows_unchanged` 计数器加 1。每一行都代表一个业务事件,不是冗余的爬虫结果。

**9 步(同一组 helper,不同编排顺序):**

1. **`parse_csv_to_stg`** —— 解析记录/文件 → INSERT 到 `stg_price_offer`(无论是否合法都保留原 payload)
2. **`run_ingest_dq`** —— INGEST 阶段 DQ(8 条规则)于原始 stg → 失败行写 `dq_output` + `dq_bad_records`;通过的标 `dq_status='INGEST_PASSED'`
3. **`harmonise_in_stg`** —— 进程内调 harmoniser → 把 `product_model_id`/`harmonise_score`/`harmonise_confidence` 写回 stg
4. **`run_prefact_dq`** —— PRE_FACT 阶段 DQ(3 条 HIGH 严重度规则)于 enriched stg → 失败行写 `dq_bad_records`;通过的标 `dq_status='PRE_FACT_PASSED'`
5. **`write_stg_to_fact(gate=…)`** —— **关键分歧点**:
   - **Path A (`gate=True`)**:只有 PRE_FACT_PASSED 行进 `fact_price_offer`,坏行被挡。fact 表 by construction 可信,下游分析不需要过滤视图。
   - **Path B (`gate=False`)**:所有可解析行都进 fact。坏行后续被 `/compute-dq` 在 `dq_bad_records` 事后标记。分析查询须 `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL` 过滤。
   - 两路径都做:国家 → ISO;时间戳 → UTC + 本地;FX 冷冻;`effective_total_local` + `effective_total_usd` 计算;INSERT `fact_price_offer` + payment 子表(CTI 设计)
6. **`run_semantic_dq`** —— SEMANTIC 阶段 DQ(2 条软信号规则)于已写入 fact 行。**失败行保留在 fact 中**(软信号需业务判断),只标记。
7. **`update_scd2`** —— SCD-2 历史协调(单 CTE)更新 `fact_partner_price_history`
8. **`detect_anomalies_for_batch`** —— STATISTICAL signal vs 30 天基线;构建 visualization payload。Path A 内联跑,Path B 通过 `/detect-anomalies` 跑。
9. **`write_batch_summary`** —— UPSERT 到 `dws_partner_dq_per_batch`,幂等。Path B 每个端点尾部都跑一次,summary 增量填充。

**Path A 调用顺序:** 1→2→3→4→5(gate=True)→6→7→8→9,单事务。
**Path B 调用顺序:** /load-data 跑 {1, 3, 5(gate=False), 7};/compute-dq 跑 {2, 4, 6};/detect-anomalies 跑 {8};每个端点尾部跑步 9。

**严重度策略。** HIGH 严重度 = 在 Path A 阻止进 fact(PRE_FACT 阶段);Path B 下事后标记。MEDIUM/LOW 严重度 = 进 fact 但打标签等待审核(SEMANTIC 阶段)。新加 gate 规则 = `dq_rule_catalog` 加一行 `target_stage='PRE_FACT'`,零代码修改。

> 业务侧的疑问"我们今天到底有没有爬 Partner X？" → 查
> `dws_partner_dq_per_batch` 即可（rows_unchanged + loaded_records 之和 =
> 当批爬到的总数）。无需在 fact_price_offer 里塞冗余行。

### B.3 POST /compute-dq — Data Quality Checks / 数据质量校验

#### EN

**Rule categories:**
- Null checks (required fields)
- Format (currency, date, country code)
- Range (positive price, instalment 1-60 months)
- Conditional (if payment_type=INSTALMENT then monthly_amount NOT NULL)
- **Duplicates** (same `(batch_id, product, partner, country, crawl_ts)` appearing >1 time)
- Harmonise confidence (LOW → bad_record)
- Cross-field consistency (currency matches country's expected currency)

**Output:**
- `dq_output` — one row per (rule, run) for trend dashboards
- `dq_bad_records` — one row per failing record; `raw_payload JSONB` preserves original even if types broke

**Business correction workflow (Task C-2):**
- Review UI filters `dq_bad_records` WHERE `status = 'NEW'`
- Each record shows raw_payload + which rule failed + severity
- Actions: RESOLVED (with notes → triggers replay of `source_batch_id`) / IGNORED / Dictionary update
- Only affected batch re-ingests — surgical, not full replay

#### 中文

**规则分类：**
- 空值检查（必填字段）
- 格式（货币、日期、国家代码）
- 范围（价格为正、分期 1-60 月）
- 条件依赖（若 payment_type=INSTALMENT 则 monthly_amount 非空）
- **重复**（相同 `(batch_id, product, partner, country, crawl_ts)` 出现多次）
- Harmonise 置信度（LOW → bad_record）
- 跨字段一致性（货币符合国家预期货币）

**输出：**
- `dq_output` —— 一条规则一次运行一行，给趋势仪表盘
- `dq_bad_records` —— 一条失败记录一行；`raw_payload JSONB` 保留原始（即使类型错也能存）

**业务纠错工作流（Task C-2）：**
- 审核 UI 过滤 `dq_bad_records WHERE status = 'NEW'`
- 每条展示 raw_payload + 违反哪条规则 + 严重度
- 操作：RESOLVED（带备注 → 触发 `source_batch_id` 回放）/ IGNORED / 更新字典
- 只重放受影响批次 —— 精准手术，不全量重跑

### B.4 POST /detect-anomalies — Anomaly Detection / 异常检测

#### EN

**Problem space:** 4 distinct anomaly types
| Type | What | Detection source |
|------|------|------------------|
| STATISTICAL | Observed price far from historical distribution | `dws_product_price_baseline_1d` |
| TEMPORAL | Sudden day-over-day jump | `fact_partner_price_history` most-recent price |
| CROSS_PARTNER | Price diverges from other partners on same product | `v_partner_price_current` |
| SKU_VARIANCE | Same product model + same partner shows multi-SKU price spread | Internal spread computation |

**Key design: per-signal independent classification (NOT combined).**
Each triggered signal produces its own row in `fact_anomaly` with its own severity. Multiple signals on same offer → multiple rows, each routed to appropriate team.

**Why:** if A and C are fine but B is severe, combining dilutes. Better to record each concern separately for independent triage.

**Signal scoring (0-1 each):**
- `statistical_score` — based on baseline mean ± stddev or p05/p95
- `temporal_score` — based on % change vs last valid price
- `cross_partner_score` — based on % diff from median of other partners

**Contextual factors:**
- `lifecycle_factor` — based on `dim_product_model.lifecycle_status`
- `event_suppression_factor` — if within `dim_market_event` window
- `category_sensitivity` — from `dim_product_category.price_volatility_class`

**Severity classification** — driven by `dim_anomaly_threshold` (all configurable):
- `severity_bar_high` → HIGH
- `severity_bar_medium` → MEDIUM
- `severity_bar_low` → LOW
- Below → not recorded

**Storage:** `fact_anomaly` with `threshold_snapshot JSONB` preserving exact config used (replay/audit proof).

**Visualization:** structured JSON (time series + baseline band + anomaly markers + cross-partner bar). Frontend renders with Chart.js / Recharts. Backend doesn't generate images — preserves interactivity and decoupling.

#### 中文

**问题空间：** 4 种异常类型
| 类型 | 含义 | 数据源 |
|------|------|--------|
| STATISTICAL | 观测价远离历史分布 | `dws_product_price_baseline_1d` |
| TEMPORAL | 日间突变 | `fact_partner_price_history` 最近价 |
| CROSS_PARTNER | 同款商品与其他 Partner 报价差距大 | `v_partner_price_current` |
| SKU_VARIANCE | 同产品同 Partner 内部有 SKU 级价差 | 内部方差计算 |

**关键设计：每信号独立分级（不合并）。**
每触发的信号独占 `fact_anomaly` 一行，带独立严重度。同一 offer 触发多信号 → 多行，各自路由到对应团队。

**为什么：** 若 A 和 C 正常但 B 严重，合并会稀释。独立记录便于定向处理。

**信号评分（0-1）：**
- `statistical_score` —— 基于基线均值±标准差或 p05/p95
- `temporal_score` —— 基于相对最近价的变化率
- `cross_partner_score` —— 基于相对其他 Partner 中位数的偏离

**情境因子：**
- `lifecycle_factor` —— 基于 `dim_product_model.lifecycle_status`
- `event_suppression_factor` —— 若落在 `dim_market_event` 抑制窗口
- `category_sensitivity` —— 来自 `dim_product_category.price_volatility_class`

**严重度分类** —— 全部由 `dim_anomaly_threshold` 驱动（可配置）：
- `severity_bar_high` → HIGH
- `severity_bar_medium` → MEDIUM
- `severity_bar_low` → LOW
- 以下 → 不记录

**存储：** `fact_anomaly` 含 `threshold_snapshot JSONB`，冷冻当时用的阈值配置（回放/审计证据）。

**可视化：** 返回结构化 JSON（时间序列 + 基线带 + 异常标记 + 跨 Partner 条形图），前端用 Chart.js / Recharts 渲染，后端不拼图（保留交互性与解耦）。

### B.5 Alert Routing / 告警路由

#### EN

| Channel | When | Why |
|---------|------|-----|
| SLACK / TEAMS | HIGH severity, immediate | Team-visible, contextual discussion |
| EMAIL | MEDIUM severity, daily digest at 09:00 | Batched, archived |
| UI only | LOW severity | Trend awareness, no push |
| WEBHOOK | Jira/Linear ticket creation | Enterprise workflow |

**Explicitly NOT used:** PHONE / SMS — overkill for non-customer-facing analysis pipeline. Avoid alert fatigue.

**Tables:** `dim_alert_channel` (registry) + `dim_alert_policy` (routing rules).

#### 中文

| 通道 | 使用时机 | 理由 |
|------|---------|------|
| SLACK / TEAMS | HIGH 严重度，即时 | 团队可见、可讨论 |
| EMAIL | MEDIUM 严重度，每日 09:00 digest | 批量、可归档 |
| UI only | LOW 严重度 | 趋势意识、不推送 |
| WEBHOOK | 对接 Jira/Linear 建工单 | 企业级工作流 |

**明确不用：** PHONE / SMS —— 本场景非客户侧、非交易系统，过度设计。避免告警疲劳。

**表：** `dim_alert_channel`（通道注册）+ `dim_alert_policy`（路由规则）。

---

## C. Technical Write-up / 技术文档

### C.1 How does the data model adapt to new partners? / 数据模型如何适配新 Partner？

#### EN
| Change | Action |
|--------|--------|
| New partner, existing payment scheme | INSERT one row in `dim_partner`. **Zero schema change.** |
| New partner, new payment type (e.g. BNPL) | 1) `ALTER TYPE payment_type_enum ADD VALUE 'BNPL';` 2) Create `fact_payment_bnpl` child table 3) Extend `effective_total_local` computation. **Existing rows untouched.** |
| New country | INSERT `dim_country` + `dim_timezone` if unseen. FX rates bootstrap `dim_currency_rate_snapshot`. **Zero fact schema change.** |
| New product category | INSERT `dim_product_category`. If new attributes needed, ADD COLUMN (dim tables tolerate this) |
| New anomaly signal | INSERT into `dim_anomaly_threshold`; add enum value; `fact_anomaly.anomaly_type` accommodates via VARCHAR |

**Fact schema stability** is the star schema's primary payoff — dim explosion doesn't cost fact migrations.

#### 中文
| 变更 | 动作 |
|------|------|
| 新 Partner，已有支付方式 | `dim_partner` 加一行。**零 schema 改动。** |
| 新 Partner，新支付方式（如 BNPL）| 1) `ALTER TYPE payment_type_enum ADD VALUE 'BNPL';` 2) 建 `fact_payment_bnpl` 子表 3) 扩展 `effective_total_local` 计算。**老数据零影响。** |
| 新国家 | 加 `dim_country` + 未见过的 `dim_timezone`。FX 汇率 bootstrap `dim_currency_rate_snapshot`。**事实表 schema 零改动。** |
| 新品类 | 加 `dim_product_category`。若需新属性，ADD COLUMN（dim 表容忍） |
| 新异常信号 | 加 `dim_anomaly_threshold` 配置；扩展 enum；`fact_anomaly.anomaly_type` 用 VARCHAR 接受 |

**事实表 schema 稳定性**是星型模型的首要收益 —— 维度爆发不引发事实表迁移。

### C.2 Error Handling & DQ Strategy / 错误处理与 DQ 策略

#### EN

**Three-tier strategy:**

**Tier 1: Automated detection**
- DQ rules run on every `/load-data` batch
- `dq_output` stores per (rule, run) aggregates
- Failing rows → `dq_bad_records` with `raw_payload JSONB`
- Low-confidence harmonise → auto-routed

**Tier 2: Business user triage (visual review interface)**
- UI filters by `status='NEW'`, assigned to domain expert
- Each record shows raw_payload + rule violation + severity
- Actions: RESOLVED (replay) / IGNORED / Dictionary update
- Audit trail: assignee + resolved_at + notes

**Tier 3: Learning loop**
- Resolved records feed abbreviation dictionary / new DQ rules / anomaly threshold calibration
- Quarterly review of `dq_output` trends to identify noisy rules

**Why this works:**
- Engineers don't know Apple product names as well as business users
- Business users act without code deploys
- Feedback closes the loop — system improves over time

#### 中文

**三层策略：**

**第 1 层：自动检测**
- DQ 规则每次 `/load-data` 都跑
- `dq_output` 存 (规则, 运行) 级别汇总
- 失败记录 → `dq_bad_records` + `raw_payload JSONB`
- 低置信 harmonise → 自动路由

**第 2 层:业务人员审核(可视化审核界面)**
- UI 按 `status='NEW'` 过滤，分配给领域专家
- 每条展示 raw_payload + 违反规则 + 严重度
- 操作：RESOLVED（触发回放）/ IGNORED / 更新字典
- 审计链：assignee + resolved_at + notes

**第 3 层：学习闭环**
- 已解决记录反馈到缩写字典 / 新 DQ 规则 / 异常阈值校准
- 季度复盘 `dq_output` 趋势，识别噪音规则

**为什么有效：**
- 工程师不如业务懂 Apple 产品命名
- 业务人员无需代码部署即可响应
- 反馈闭环 —— 系统越用越准

### C.3 Scaling /load-data to 1M records / 扩展到百万级加载

#### EN

**Bottleneck analysis:** Current synchronous flow takes 3-7 hours for 1M records because of per-row DB (database) round-trips — INSERT, FX (foreign exchange) rate lookup, harmonise call, DQ (data quality) check, SCD-2 (Slowly Changing Dimension Type 2) update. HTTP (Hypertext Transfer Protocol) connection times out far earlier than that.

**Redesign — 7 key changes:**

1. **Async accept (HTTP 202 + job_id):** Upload file to AWS S3 (Amazon Simple Storage Service), then return `HTTP 202 Accepted` plus a `job_id` immediately. Client polls `GET /load-data/{job_id}` for progress. Decouples HTTP request lifecycle from processing time.
2. **COPY instead of INSERT:** PostgreSQL's `COPY FROM STDIN` bypasses SQL parsing and is 50-100× faster than per-row INSERT for bulk loads.
3. **Chunk-and-parallelize:** Split file into 10k-row chunks, dispatch to a worker pool via a queue (Redis or AWS SQS — Simple Queue Service). Workers run in separate processes to bypass Python's GIL (Global Interpreter Lock) for CPU-bound work.
4. **In-memory reference data:** Preload `dim_partner` / `dim_country` / `dim_currency` / `dim_currency_rate_snapshot` and the harmonise registry into worker memory on startup. Zero per-row DB lookups during processing.
5. **Batch DQ in SQL:** INGEST-stage rules are already implemented as `dq_check_*(batch_id)` SQL functions — one call per rule per chunk, Postgres vectorizes internally rather than iterating per row in Python.
6. **Bulk SCD-2 via single SQL:** Post-batch, one CTE-based (Common Table Expression) `INSERT` statement handles all price history changes for the batch.
7. **Observability:** `ingest_job` table tracks `chunk_count` / `completed_chunks` / `rows_loaded` / `rows_bad`. Progress endpoint reads this table; client polls it for ETA (estimated time of arrival).

**Performance targets:**
- 1M rows: current 3-7 hours → **target ~90 seconds** (~200×).
- 10M rows: not feasible under synchronous flow → ~15 minutes with partition-aware sharding.

**Failure handling:**
- Chunk-level idempotency via `(source_batch_id, row_num)` UNIQUE constraint + `ON CONFLICT DO NOTHING`
- Retry up to 3× then move to a DLQ (Dead Letter Queue)
- `ingest_job.status = 'PARTIAL'` if some chunks failed — business decides to keep or rollback
- Clean rollback: `DELETE ... WHERE source_batch_id = $1` removes the entire batch's footprint

**Migration path (incremental adoption):** A) enable `COPY` → B) chunking + reference-data caching → C) async queue + `HTTP 202` accept → D) parallel workers → E) full post-batch SQL pipeline. Demo aims for B; road to E clearly documented.

#### 中文

**瓶颈分析：** 当前同步流程处理 100 万行需要 3-7 小时，瓶颈在于**逐行 DB（数据库）round-trip** —— INSERT、FX（Foreign Exchange，外汇）查询、Harmonise 调用、DQ（Data Quality，数据质量）校验、SCD-2（Slowly Changing Dimension Type 2，缓慢变化维度第 2 型）更新。HTTP（Hypertext Transfer Protocol）连接早就超时了。

**重构 —— 7 项关键改动：**

1. **异步接受（HTTP 202 + job_id）：** 文件上传到 AWS S3（Amazon Simple Storage Service，亚马逊简单存储服务）后立即返回 `HTTP 202 Accepted` 与一个 `job_id`。客户端用 `GET /load-data/{job_id}` 查进度。把 HTTP 请求生命周期与处理时间解耦。
2. **COPY 替代 INSERT：** PostgreSQL 的 `COPY FROM STDIN` 绕过 SQL 解析，批量导入比逐行 INSERT **快 50-100 倍**。
3. **分片 + 并行：** 把文件切成 10k 行一片，通过队列（Redis 或 AWS SQS — Simple Queue Service）分发给 worker pool（工作进程池）。Worker 用多进程（非线程）规避 Python 的 GIL（Global Interpreter Lock，全局解释器锁），让 CPU 密集的工作真正并行。
4. **参考数据内存预载：** `dim_partner` / `dim_country` / `dim_currency` / FX 汇率 / Harmonise registry 在 worker 启动时一次性加载到内存。**处理时零 per-row DB 查询。**
5. **批量 DQ 走 SQL：** INGEST 阶段规则已经实现成 `dq_check_*(batch_id)` SQL 函数 —— 每 chunk 每规则只调一次，Postgres 内部向量化处理，而不是在 Python 里 per-row 迭代。
6. **SCD-2 单 SQL 批量：** Post-batch 阶段用一条 CTE-based（Common Table Expression，公共表表达式）`INSERT` 语句处理整批所有价格历史变化。
7. **可观测性：** `ingest_job` 表追踪 `chunk_count` / `completed_chunks` / `rows_loaded` / `rows_bad`。进度端点直接读这张表，客户端轮询查 ETA（Estimated Time of Arrival，预计完成时间）。

**性能目标：**
- 100 万行：从现在 3-7 小时 → **目标 ~90 秒**（~200 倍加速）
- 1000 万行：同步不可行 → 按 partner + date 分片后 ~15 分钟

**失败处理：**
- Chunk 级幂等：`(source_batch_id, row_num)` UNIQUE 约束 + `ON CONFLICT DO NOTHING`
- 最多 3 次重试，仍失败进 DLQ（Dead Letter Queue，死信队列）
- 部分失败时 `ingest_job.status = 'PARTIAL'` —— 业务决定保留或回滚
- 干净回滚：`DELETE ... WHERE source_batch_id = $1` 一句话清理整批痕迹

**演进路径（可逐步落地）：** A) 启用 `COPY` → B) 分片 + 参考数据缓存 → C) 异步队列 + `HTTP 202` 接收 → D) 并行 worker → E) 完整 post-batch SQL 流水线。演示做到 B，其余清楚写在路线图里。

---

## Appendix: Calibration Methodologies / 附录：校准方法论

### Principle / 原则

#### EN
Every "magic number" lives in `dim_anomaly_threshold` with:
- `rationale` — human explanation
- `source` — `'initial_guess' | 'data_calibrated' | 'business_agreed'`
- `last_reviewed_at` — freshness indicator

Placeholders marked `'initial_guess'` are temporary. All have calibration plans below.

#### 中文
每个"神奇数字"都在 `dim_anomaly_threshold` 配置表里，带：
- `rationale` —— 人类可读解释
- `source` —— `'initial_guess' | 'data_calibrated' | 'business_agreed'`
- `last_reviewed_at` —— 更新时间

标记为 `'initial_guess'` 的是临时占位。下面每一项都有校准计划。

### Cal.1 Anomaly Signal Weight Calibration (Logistic Regression) / 信号权重校准

**Placeholder / 占位值：** `0.4 / 0.3 / 0.3` (statistical / temporal / cross_partner).

**Method / 方法：**
1. Collect 3+ months of `fact_anomaly` rows / 收集 3+ 月的 `fact_anomaly` 数据
2. Business labels each: TRUE_POSITIVE / FALSE_POSITIVE / 业务标注每条：真阳 / 假阳
3. Fit Logistic Regression:
   - Features: `signal_statistical, signal_temporal, signal_cross_partner`
   - Target: `is_true_positive`
   逻辑回归拟合，特征 = 三个信号分，目标 = 是否真阳
4. Use learned coefficients as weights / 学习到的系数作为新权重
5. Write to `dim_anomaly_threshold` (keys `signal_weight_*`) / 写回配置表
6. Re-calibrate quarterly / 每季度重校

**Benefits / 好处：** data-driven, interpretable, extensible. / 数据驱动、可解释、易扩展。

### Cal.2 Severity Threshold Calibration (Business Capacity) / 严重度 bar 校准

**Placeholder / 占位值：** HIGH=0.80, MEDIUM=0.50, LOW=0.25.

**Method / 方法：**
1. Measure team daily capacity (N records/day) / 度量业务团队日处理能力
2. Target volume: HIGH ~5%, MEDIUM ~15% / 目标占比：HIGH 5%、MEDIUM 15%
3. Query historical `fact_anomaly.final_score` distribution / 查历史分数分布
4. HIGH bar = p95 score / MEDIUM bar = p80 / HIGH 阈值 = p95，MEDIUM = p80
5. Re-calibrate when team size changes / 团队人手变化时重校

### Cal.3 Cross-Partner Variance Threshold / 跨 Partner 价差阈值

**Placeholder / 占位值：** 5% / 15% / 25%.

**Method / 方法：** historical percentile derivation / 历史分位数推导

```sql
SELECT
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pct_diff) AS p50,
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pct_diff) AS p90,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pct_diff) AS p99
FROM (
  SELECT (MAX(price_usd) - MIN(price_usd)) / AVG(price_usd) AS pct_diff
  FROM fact_partner_price_history
  WHERE valid_to_date IS NULL
  GROUP BY product_model_id, country_code
  HAVING COUNT(DISTINCT partner_id) >= 2
);
```

Use p90 → suspicious; p99 → severe. / p90 为可疑线，p99 为严重线。

### Cal.4 Temporal Change Threshold / 时间突变阈值

**Placeholder / 占位值：** 5% / 10% / 20% / 30%.

**Method / 方法：** Same percentile approach on day-over-day price changes from `fact_partner_price_history`. Tiers at p50 / p90 / p99.
同分位数方法，对日间价格变化率。分档取 p50 / p90 / p99。

### Cal.5 Category Volatility Class (Coefficient of Variation) / 品类波动系数

**Placeholder / 占位值：** LOW / MEDIUM / HIGH with multipliers 1.0 / 1.3 / 1.8.

**Method / 方法：**
```sql
SELECT category_code,
       STDDEV(stddev_price_usd / NULLIF(mean_price_usd, 0)) AS cv
FROM dws_product_price_baseline_1d b
JOIN dim_product_model m USING (product_model_id)
JOIN dim_product_category c ON m.category_id = c.category_id
WHERE b.window_days = 30
GROUP BY category_code;
```

Tercile split: bottom → LOW, middle → MEDIUM, top → HIGH. Re-calibrate monthly.
三分位切：底部 LOW、中部 MEDIUM、顶部 HIGH。每月重校。

### Cal.6 Lifecycle Auto-Derivation / 生命周期自动派生

**Placeholder / 占位值：** manual STABLE default.

**Method (daily cron) / 方法（每日 cron）：**
```sql
UPDATE dim_product_model m SET lifecycle_status =
  CASE
    WHEN m.launch_date > CURRENT_DATE - INTERVAL '14 days' THEN 'NEW'
    WHEN m.eol_date IS NOT NULL AND m.eol_date < CURRENT_DATE THEN 'EOL'
    WHEN m.launch_date + (cat.lifecycle_months - 3) * INTERVAL '1 month' < CURRENT_DATE THEN 'LEGACY'
    ELSE 'STABLE'
  END
FROM dim_product_category cat
WHERE m.category_id = cat.category_id;
```

`lifecycle_months` from `dim_product_category` (iPhone=12, iPad=18, Mac=24). Aligns with Apple refresh cadence.
`lifecycle_months` 来自 `dim_product_category`（iPhone=12、iPad=18、Mac=24），对齐 Apple 发布周期。

### Cal.7 Event Suppression Factors / 事件抑制因子

**Placeholder / 占位值：** APPLE_LAUNCH = 0.40, BLACK_FRIDAY = BOXING_DAY = 0.50.

**Method (A/B test) / 方法（A/B 测试）：**
1. Over 3 months, run detection with/without suppression on known events
   3 个月，用/不用抑制，对已知事件跑检测
2. Measure FP rate at factors [0.3, 0.4, 0.5, 0.6]
   在因子 [0.3, 0.4, 0.5, 0.6] 下测量误报率
3. Pick factor minimizing FP while preserving TP recall for non-event anomalies
   选误报最低、同时不削弱非事件异常召回的因子

---

## Design Decisions Log / 设计决策日志

| Decision / 决策 | Why / 理由 |
|----------|-----|
| Postgres (not MongoDB/SQLite) / 选 PG 不选 Mongo/SQLite | Rich partitioning, JSONB, native TZ handling / 分区、JSONB、原生时区支持 |
| Harmonise at model level (not SKU) / 在型号级 harmonise 而非 SKU 级 | Partner data lacks color; sample size matters / Partner 没颜色；样本量要紧 |
| CTI over JSONB for payment / 支付用 CTI 而非 JSONB | Type safety + no sparse columns / 类型安全 + 无稀疏列 |
| Monthly partitions (not daily) / 按月分区而非按日 | Apple scale fits; PG 14+ handles 1000+ partitions fine / Apple 量级舒适；PG 14+ 分区数无忧 |
| SCD Type 2 price history / SCD-2 价格历史 | 90% of partner "new" observations are duplicates / 90% 新观测是重复 |
| Per-signal anomaly rows / 每信号独立一行 | Each concern deserves independent routing / 每种问题独立路由处理 |
| No PHONE/SMS alerts / 不用电话/短信告警 | Non-customer-facing, hour-level OK, avoid fatigue / 非客户面、小时级可接受、避免疲劳 |
| All thresholds in config table / 所有阈值进配置表 | Business iteration > code deploys / 业务迭代优先于代码部署 |

---

## Presentation Slide Ideas / 演示材料思路

### EN (1h demo + 30min Q&A)
1. **The "Hard" Problems** (5 min) — Abbreviation chaos, payment diversity, multi-stakeholder timezones, storage vs freshness tradeoff
2. **Data Model Walk-Through** (15 min) — Star schema, CTI pattern, SCD-2, A+MV hybrid
3. **Harmonise Demo** (10 min) — Show `iP 17 PM 512GB` → score → top-K; LOW confidence flow
4. **Anomaly Demo** (15 min) — Sample detection, severity routing, visualization, event suppression
5. **Data Quality Loop** (5 min) — review UI sketch, replay mechanism
6. **Scaling Story** (10 min) — pg_partman, calibration methodology, event-driven alerts
7. **Challenges & Learnings** — Schema iterations, threshold calibration

### 中文（1 小时演示 + 30 分钟问答）
1. **硬问题拆解**（5 min）—— 缩写混乱、支付方式多样、多角色时区、存储 vs 新鲜度权衡
2. **数据模型走查**（15 min）—— 星型、CTI、SCD-2、A+MV 混合
3. **Harmonise 演示**（10 min）—— `iP 17 PM 512GB` → 打分 → Top-K；低置信流程
4. **异常检测演示**（15 min）—— 样本检测、严重度路由、可视化、事件抑制
5. **DQ 闭环**(5 min)—— 审核 UI 草图、回放机制
6. **扩展性故事**（10 min）—— pg_partman、校准方法论、event-driven 告警
7. **挑战与收获** —— Schema 迭代、阈值校准过程
