# Demo Runbook · 面试现场操作手册

私人 Cheat Sheet,与 [`presentation.md`](presentation.md) + [`demo_queries.sql`](demo_queries.sql) 配合使用。
每节给出**做什么(中文) + 说什么(英文台词 + 中文备注) + 指什么**。

时长预估:**~50 分钟讲述 + 30 分钟 Q&A**。

**整体演讲结构:**

| § | 主题 | 时长 |
|---|---|---:|
| 1 | 项目背景 + 题目要求 + 我实现了什么 | 3 min |
| 2 | 整体 App 演示(上传文件 → DB 全表) | 10 min |
| 3 | 技术架构图 + 业务流程图 | 5 min |
| 4 | **Task A · 数据库设计**(需求→schema 映射) | 6 min |
| 5 | Task B 4 个核心接口详细设计 | 15 min |
| 6 | Task C 三个回答(C-1 / C-2 / C-3) | 6 min |
| 7 | 反思:挑战 / 亮点 / 可优化 | 8 min |
| 8 | Q&A | 30 min |

---

## 飞行前检查(开始前 15 分钟)

```bash
# 1. Postgres + 三阶段规则齐全
psql -d maya_assignment -c "SELECT target_stage, COUNT(*) FROM dq_rule_catalog GROUP BY 1 ORDER BY 1;"
# 期望: INGEST=8, PRE_FACT=3, SEMANTIC=2(共 13 条)

# 2. 测试全绿
cd "Apple SDE" && python3 -m pytest -q
# 期望: 39 passed
```

**3. 重置 DB 到干净 baseline**(强烈推荐 — 让现场数字和 [`results_showcase.html`](../Apple%20SDE/submission/results_showcase.html) 对得上)

```bash
PSQL=/Applications/Postgres.app/Contents/Versions/latest/bin/psql

# 清空所有事实/staging/DQ 数据(维度表不动)
$PSQL -d maya_assignment <<'SQL'
TRUNCATE TABLE
  fact_payment_full_price, fact_payment_instalment, fact_price_offer,
  fact_partner_price_history, stg_price_offer, dq_bad_records, dq_output,
  dws_partner_dq_per_batch
RESTART IDENTITY CASCADE;
SQL

# 重新加载 Partner A + B 各一次
curl -X POST http://localhost:8000/load-data -F "file=@Partner A.csv" -F "partner_code=PARTNER_A"
curl -X POST http://localhost:8000/load-data -F "file=@Partner B.csv" -F "partner_code=PARTNER_B"
```

**预期 baseline 行数:** stg 4 208 / fact 4 174 / history 119 / dq_bad_records 188 / dq_output 26(2 batches × 13 rules)

> ⚠️ Demo 中如果手抖多次上传 Partner A 或 B,大部分表会**累积**(stg / fact / dq_*),只有 `fact_partner_price_history` 因 SCD-2 同日守卫不会变。`results_showcase.html` 是静态快照不受影响,但 VSCode 里的全表 COUNT 会变大 —— 演讲前 reset 一次就稳了。

**Tab 配置(2 终端 + VSCode 数据库面板):**

| 位置 | 用途 | 启动命令 |
|---|---|---|
| **Terminal Tab ①** | uvicorn(全程不动) | `lsof -ti:8000 \| xargs kill 2>/dev/null; python3 -m uvicorn api.main:app --port 8000` |
| **Terminal Tab ②** | 备用 shell(跑 pytest / 临时命令 / 翻车应急) | `cd "Apple SDE"`(等用) |
| **VSCode 数据库面板** | 实时查 DB | 打开 [`demo_queries.sql`](demo_queries.sql) 选段执行 |
| **浏览器** | Swagger UI | `http://localhost:8000/docs` |

**VSCode Database Client 连接:** `localhost / 5432 / whoami / 留空 / maya_assignment`

**屏幕分享:** 主屏 = VSCode(代码 + SQL 同屏);切 Tab ② 触发 API;偶尔切 Tab ① 让面试官看 INFO 日志。

---

## §1 项目背景 + 题目要求 + 我实现了什么(3 min)

**对应:** [`presentation.md`](presentation.md) Slide 1–3。

**台词:**
> *"GetGround asked for a pricing reconciliation pipeline. Two partner stores ship product-pricing CSVs in different shapes — Partner A uses instalment columns, Partner B uses single full-price; country codes inconsistent. The task has three parts: (A) database design that reconciles the two sources without sparse columns, (B) 4 RESTful endpoints (load-data, compute-dq, detect-anomalies, harmonise-product), (C) three short write-ups on data-model adaptation, error handling, and 1 M-row scaling."*
>
> **中文备注:** GetGround 题目要求 Apple 经销商价格调和;Partner A/B 数据格式各异;Task A=数据模型,Task B=4 个 REST 接口,Task C=3 个短答(扩展性 / DQ 闭环 / 1M 扩展)。

**我交付了什么(一口气快速过):**
- ✅ Task A — 29 张表,Class Table Inheritance + Slowly Changing Dimension Type 2 + 三层 DQ 表
- ✅ Task B — 4 个接口全部实现,真 PostgreSQL 后端,**39/39 测试通过**
- ✅ Task C — 三个短答都在 `submission/task_c_answers.md` 里
- 加分项:DQ 三阶段(INGEST + PRE_FACT gate + SEMANTIC),解决了"fact 表是否可信"的根本问题

**指:** Slide 2 Data Volume 表 — 设计是为每天 80–160 K 观测,不只是 4 K 样本。

---

## §2 整体 App 演示(10 min · 核心)

**目标:** 让面试官**看到**整个流水线在真 DB 上跑通。**全程在浏览器里用 Swagger UI 操作**,无需切 terminal 跑 curl。

### 2a · 打开 Swagger UI,介绍 9 个端点 + 双路径

浏览器打开 `http://localhost:8000/docs`,**从上往下滚一遍**,简短说明每个端点:

```
[Path A — 一键编排]
POST /pipeline                        ← orchestrator,9 步流水线一键跑

[Path B — Task B 4 个独立子模块]
POST /load-data                       ← Task B-1: parse + harmonise + 写 fact (no gate)
GET  /load-data/{job_id}              ← 异步进度查询
POST /compute-dq                      ← Task B-2: 13 条 DQ → 2 张表
POST /detect-anomalies                ← Task B-3: 异常检测 + visualization
GET  /harmonise-product               ← Task B-4: 单条 harmonise

[支撑端点]
GET  /bad-records                     ← Task C-2 业务审核
POST /bad-records/{id}/resolve        ← 解决/标记 bad record
GET  /health                          ← Liveness probe
```

**台词:**
> *"Two call paths sharing the same 9 internal step helpers. **Path A `/pipeline`** is the one-click orchestrator — runs the full pipeline end-to-end with a hard PRE_FACT gate that blocks bad rows from `fact_price_offer`. **Path B** is the 4 Task-B sub-modules — each independently callable; same 9 steps but the gate degrades to post-hoc flagging. Both paths call the same Python helpers — zero duplication. Path A trades flexibility for stronger guarantees; Path B trades guarantees for granular control."*
>
> **中文备注:** 两条路径共用 9 个内部 helper。Path A `/pipeline` 一键编排带硬 gate;Path B 是 Task B 要求的 4 个独立子模块,gate 退化为事后标记。零代码重复。

### 2b · Partner A 走 Path A — `POST /pipeline`(一键 9 步)

**故意用 Path A 演示一键流水线 + 硬 gate**:

1. 点击 **`POST /pipeline`** 展开
2. 点右上角 **"Try it out"** 进入交互模式
3. **`file`** 字段 → 选 `Apple SDE/Partner A.csv`
4. **`partner_code`** 填 `PARTNER_A`
5. 点蓝色 **"Execute"**

响应是 **HTTP 200** + 聚合结果(单次返回就包含 dq + anomaly):

```json
{
  "job_id": "...",
  "source_batch_id": "<UUID>",
  "rows_loaded": 3108,
  "rows_bad": 106,
  "dq_summary": { "total_violations": 106, "by_severity": {...} },
  "anomalies_total": ...,
  ...
}
```

**复制 `source_batch_id`** —— 一会儿在 §2d highlight。

**台词:**
> *"One call, full 9-step pipeline inside a single Postgres transaction. `rows_loaded=3108`, `rows_bad=106` — those 106 PRE_FACT-failing rows are blocked from `fact_price_offer`. The fact table for this batch is **trustworthy by construction**; downstream analytics can hit it directly without filter views. This is Path A's contract."*

### 2c · Partner B 走 Path B — 三步分调,讲 NZ 故事 + 事后标记

**故意用 Path B 让面试官看到子模块独立可调 + 标记语义对比**:

**Step 1** — `POST /load-data`(只 load,不跑 DQ)
- **`file`** → `Partner B.csv` / **`partner_code`** → `PARTNER_B` → **Execute**
- 复制响应里的 `source_batch_id`

**Step 2** — `POST /compute-dq`(独立跑 DQ)
- Request body:
```json
{ "source_batch_id": "<刚才复制的>" }
```
- 响应展示 13 条规则 pass-rate + 各 severity 命中数

**Step 3** — `POST /detect-anomalies`(独立跑异常检测)
- Request body:
```json
{ "source_batch_id": "<上面同一个>", "min_severity": "MEDIUM" }
```

**台词(双重对比):**
> *"Path B is the 4 Task-B sub-modules called individually — what the assignment literally specified. **Critical contrast with Path A**: in Path B, `/load-data` writes everything to fact BEFORE DQ runs, so `/compute-dq` flags bad rows post-hoc — they stay in `fact_price_offer` with a marker in `dq_bad_records`. Analytics queries on Path B's fact need `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL` to filter them out. This is the price of decoupling — and it's why `/pipeline` exists as the alternative for production."*
>
> **NZ 故事(同时讲):** *"Partner B has 154 rows where COUNTRY_VAL is `'NZ'` instead of `'New Zealand'`. Earlier `DQ_FMT_001` only accepted full names — those 154 failed. The fix was a one-line dictionary extension; replay the same `source_batch_id` and they promote into fact. Real example of the DQ → rule iteration → replay loop, Task C-2 in action."*
>
> **中文备注:** Path B 三步分调 = Task B 字面要求;关键对比是 fact 表"事后标记"vs Path A 的"硬 gate"。NZ 154 行是 DQ → 规则迭代 → replay 闭环的真实例子。

### 2d · 切到浏览器,打开 [`results_showcase.html`](../Apple%20SDE/submission/results_showcase.html) 走 **7 张可视化卡片**

```bash
open "Apple SDE/submission/results_showcase.html"
```

按顺序滚动讲(每张图大约 30–60 秒):

| 区块 | 你要讲什么 |
|---|---|
| **Headline** 4 个数字大卡 | "4 174 fact rows / 90% HIGH harmonise / 9 blocked / 188 violations —— 一行能看完整体" |
| **① Pipeline Funnel** | stg 4208 → fact 4174,中间 PRE_FACT gate 挡掉 9 行(下面琥珀色框讲那 9 行全是 Apple Watch SKU,Reference 没收录) |
| **② Harmonise Confidence Distribution** | 90% HIGH —— 算法在脏数据上的真实命中率 |
| **③ DQ Pass-Rate by Rule** | 13 条规则 10 条 100% 通过,3 条触发(DQ_HARM_001 / DQ_HARM_002 / DQ_PRICE_001) |
| **④ Anomaly Visualization** | 时序 + 基线带 + 红色异常点 —— 这就是返给前端的 JSON 渲出来的样子 |
| **⑤ Harmonise 真实样例** | `iP15P 128` HIGH 0.845 / `iP 17 PM 512GB` HIGH 0.946 / 长冗余命名也 HIGH —— 三信号 breakdown |
| **⑥ Sample Bad Records** | 真实 raw_payload + Apple Watch 被挡的 2 条 + AirPods Pro 2 低置信 + iPhone 价格异常 |
| **⑦ NZ 154-Rows 故事** | DQ → 规则迭代 → replay 闭环的真实例子(154 → 0) |

**台词收尾:**
> *"Notice the funnel: every stg row preserved with raw_payload, but only PRE_FACT-passing rows enter fact_price_offer. So `SELECT * FROM fact_price_offer` is safe directly — analytics never need a filter view."*
>
> **中文备注:** 漏斗 stg→fact;HIGH 错误被 PRE_FACT 挡掉;fact 表本身可信。

**(可选)切到 VSCode 跑 1 条 SQL 证明"这不是预渲染的 mock"** —— `demo_queries.sql` §0 三条 sanity check,30 秒,展示 DB 真的响应同样的数字。其他 SQL 段(§3 漏斗 / §4.2 DQ_HARM_002 拦截)**可跳过**——HTML 已经把这些可视化好了,SQL 跑出表格反而不如图直观。

> 💡 **`demo_queries.sql` 在 demo 中的新角色:**
> - HTML showcase = 主要展示面(故事 + 视觉)
> - VSCode SQL = drill-down 工具(面试官追问"那条 SQL 是什么"时打开)

---

## §3 技术架构图 + 业务流程图(5 min)

### 3a · 技术架构图(分层视图)

```
┌────────────────────────────────────────────────────────────────────┐
│                         Client / Interviewer                       │
│            Swagger UI · curl · future Frontend (React)             │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ HTTP/JSON
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  API Layer · FastAPI + Pydantic 2 (api/main.py)                   │
│                                                                    │
│   ▸ Path A (orchestrator):  POST /pipeline                         │
│   ▸ Path B (Task B 子模块): /load-data /compute-dq                 │
│                              /detect-anomalies /harmonise-product  │
│   ▸ 支撑端点:                /bad-records, /load-data/{id}, /health│
└──────────────────────────────┬─────────────────────────────────────┘
                               │ 两条路径都调下面同一组 helper
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  Service Layer · Async Python (api/services.py)                    │
│                                                                    │
│  9 step helpers (共享):                                             │
│   1. parse_csv_to_stg            6. run_semantic_dq                │
│   2. run_ingest_dq               7. update_scd2                    │
│   3. harmonise_in_stg            8. detect_anomalies_for_batch     │
│   4. run_prefact_dq              9. write_batch_summary            │
│   5. write_stg_to_fact(gate=…)                                     │
│                                                                    │
│  Service entry points:  run_pipeline (Path A) /                    │
│                          submit_load_job · compute_dq_service ·    │
│                          detect_anomalies_service (Path B)         │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ asyncpg pool
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  Data Layer · PostgreSQL 14+                                       │
│                                                                    │
│  ▸ Dimensions (14):  dim_country / dim_partner / dim_product_*    │
│                      dim_currency_rate_snapshot · dim_anomaly_*    │
│  ▸ Facts (5):        fact_price_offer (Class Table Inheritance)   │
│                      fact_payment_full_price · fact_payment_*      │
│                      fact_partner_price_history (Slowly Changing   │
│                                                  Dimension Type 2) │
│                      fact_anomaly                                  │
│  ▸ DQ (3):           dq_rule_catalog (13 rules) · dq_output        │
│                      dq_bad_records (workflow + raw_payload JSONB) │
│  ▸ DWS:              materialized view + summary tables            │
└────────────────────────────────────────────────────────────────────┘
```

**台词:**
> *"Three-layer architecture. The API layer offers two paths: an orchestrator endpoint `/pipeline` that runs all 9 steps end-to-end, and the 4 Task-B sub-modules that are independently callable. Both paths invoke the **same 9 helper functions** in the service layer — zero duplication, just different orchestration. Service layer holds pure-Python algorithms (harmoniser, anomaly detector) and orchestrates SQL calls. Data layer is PostgreSQL with a 29-table star schema. asyncpg pool gives us concurrent queries without thread overhead."*

### 3b · 业务流程图(9 步 · 双路径)

```
   Partner CSV upload + partner_code
           │
           ▼
   ┌─────────────────────┐
   │ ① Parse → stg       │  raw_payload JSONB 全保留
   └──────────┬──────────┘
              │
              ▼
   ┌─────────────────────┐
   │ ② INGEST DQ (8)     │  null / format / range / conditional
   └──────────┬──────────┘
              │ 通过的标 dq_status='INGEST_PASSED'
              ▼
   ┌─────────────────────┐
   │ ③ Harmonise         │  Top-K 三信号匹配,写回 product_model_id
   └──────────┬──────────┘
              │
              ▼
   ┌─────────────────────┐
   │ ④ PRE_FACT DQ (3)   │  HIGH 严重度规则:country↔currency /
   │                     │  partner↔country / harmonise unmatched
   └──────────┬──────────┘
              │ 失败行 → dq_bad_records
              │ 通过的标 dq_status='PRE_FACT_PASSED'
              ▼
   ┌─────────────────────┐
   │ ⑤ write_stg_to_fact │  ⚠️ 关键分歧点:
   │                     │
   │  Path A: gate=True  │   只写 PRE_FACT_PASSED 行
   │    → fact 干净       │   → fact_price_offer (Class Table
   │                     │     Inheritance: full_price /
   │                     │     instalment 子表)
   │                     │
   │  Path B: gate=False │   写所有可解析行(包括坏行)
   │    → fact 含标记行   │   → 后续 LEFT JOIN dq_bad_records 过滤
   └──────────┬──────────┘
              │
              ▼
   ┌─────────────────────┐
   │ ⑥ SEMANTIC DQ (2)   │  软信号(单行可判断):
   │  flag-and-keep      │  低置信 harmonise / 价格 sanity 范围
   └──────────┬──────────┘
              │ 失败行**留在 fact**,只进 dq_bad_records 标记
              ▼
   ┌─────────────────────┐
   │ ⑦ Slowly Changing   │  单条 CTE: latest → existing →
   │   Dimension Type 2  │  changed → closed → insert
   │                     │  → fact_partner_price_history
   └──────────┬──────────┘
              │
              ▼
   ┌─────────────────────┐
   │ ⑧ detect_anomalies  │  STATISTICAL signal (vs 30-day baseline)
   │                     │  visualization payload (series + band)
   └──────────┬──────────┘
              │
              ▼
   ┌─────────────────────┐
   │ ⑨ Batch summary     │  → dws_partner_dq_per_batch
   │                     │  (UPSERT,Path B 增量写入)
   └─────────────────────┘

   ════════════ 双路径调用映射 ════════════════════════════════════════

   Path A — POST /pipeline    单事务 9 步全跑,顺序: 1→2→3→4→5(gate)→6→7→8→9
                              fact 干净;PRE_FACT 失败行不入 fact

   Path B — 4 个子模块分调:
     POST /load-data            步 1, 3, 5(no gate), 7
     POST /compute-dq           步 2, 4, 6
     POST /detect-anomalies     步 8
     每个端点尾部 UPSERT 步 9 → 共同填满 summary 表
                              fact 含坏行;LEFT JOIN dq_bad_records 过滤
```

**台词:**
> *"9 steps shared by both paths. Path A `/pipeline` runs them inside a single Postgres transaction in interleaved order — INGEST DQ before harmonise, PRE_FACT before fact write, SEMANTIC after. The PRE_FACT gate at step 5 hard-blocks bad rows from `fact_price_offer`. Path B's 4 sub-modules cover the same 9 steps but grouped — `/load-data` does the load-related steps without DQ, `/compute-dq` does all 3 DQ stages post-hoc, `/detect-anomalies` runs the anomaly signal. Same code; different transactional boundaries; observable difference in fact-table cleanliness."*
>
> **中文备注:** 同一组 9 个 helper,两条路径不同顺序。Path A 单事务交错跑、硬 gate;Path B 分组分多事务、事后标记。同代码不同语义,fact 表干净度可观测差异。

---

## §4 Task A · 数据库设计(6 min)

**对应:** [`presentation.md`](presentation.md) Slide 7–9。

讲解策略:**先用一张映射表把题目 5 项要求和我们的设计对上号**,再深入两个最有亮点的设计(CTI 和 SCD-2)。

### 4a · 题目要求 → 设计映射(看 Slide 7)

| 题目要求 | 我的设计 | 关键表/概念 |
|---|---|---|
| 1. 调和不同 partner 数据格式 | 单一规范化 fact 表 + harmonise 流水线把原始名映射到标准 model | `fact_price_offer` + `dim_product_model` |
| 2. 多支付方式 **不要 sparse 列** | **Class Table Inheritance(CTI)**:父表 + 1:1 子表分支 | `fact_price_offer` + `fact_payment_full_price` / `fact_payment_instalment` |
| 3. 标准化 product ID | 双键:SERIAL 代理键(用于 join)+ VARCHAR 自然键(用于幂等) | `dim_product_model.product_model_id` + `model_key` |
| 4. 时序追踪 | Bi-temporal 事实(business + system 时间)+ **SCD-2** 历史 | `fact_price_offer.{crawl_ts_utc, ingested_at}` + `fact_partner_price_history` |
| 5. dq_output / bad_records 表 | 三层 DQ 表:catalog 注册 + per-(rule,run) 汇总 + per-failing-row 明细 | `dq_rule_catalog` + `dq_output` + `dq_bad_records` |

**台词:**
> *"Five requirements, five deliberate design responses. Each row of this table answers 'why this not that'. Class Table Inheritance for payment polymorphism, not sparse columns. SCD-2 for history, not overwrite-in-place. Three-tier DQ tables, not one unified dump. Each choice has a clear cost we accepted to gain a specific property."*
>
> **中文备注:** 5 项需求 5 个对应设计;每一项都是有意识的取舍——为什么这样不那样。

**核心数字:** 29 张表 / 4 个 ENUM / 月分区 / A+MV 聚合混合 / 完整精简 schema 在 `submission/task_a_schema.sql`。

### 4b · Star Schema 走一下(看 Slide 8)

打开 [`schema.sql`](../Apple%20SDE/schema.sql) 翻到 SECTION 2,**指着** `fact_price_offer` 的 CTI 父子关系:

```sql
-- 父表 (CTI 父)
CREATE TABLE fact_price_offer (
  payment_type  payment_type_enum NOT NULL,   -- discriminator
  ...
);

-- 子表 1
CREATE TABLE fact_payment_full_price (
  full_price NUMERIC(12,2) NOT NULL CHECK (full_price > 0),  -- 真 NOT NULL!
  ...
);

-- 子表 2
CREATE TABLE fact_payment_instalment (
  monthly_amount     NUMERIC(12,2) NOT NULL CHECK (monthly_amount > 0),
  instalment_months  SMALLINT      NOT NULL CHECK (instalment_months BETWEEN 1 AND 60),
  ...
);
```

**台词:**
> *"This is the payoff: each child has real NOT NULL + CHECK constraints, not the NULLable columns you'd be forced into with a sparse single-table design. Adding a new payment method like Buy Now Pay Later is `ALTER TYPE` plus one new child table — old data zero impact."*
>
> **中文备注:** CTI 的回报:子表的 NOT NULL/CHECK 约束是真的;加新支付类型(BNPL)只是 ALTER TYPE + 加一张子表,老数据零影响。

### 4c · SCD-2 + A+MV 混合(看 Slide 9)

讲 `fact_partner_price_history` 怎么做"压缩历史":

- 价格 1 周变 1 次,每天 4 次爬取 → 360 行原始观测
- SCD-2 只在价格**真变化**时插一行 → 同期 ~3-5 行
- **20-50× 压缩比**

**台词:**
> *"Partner prices change roughly weekly, but partners are scraped 4-12 times per day. Storing every observation is 90% redundant. SCD-2 inserts only on actual change, with valid_from / valid_to date ranges — a compressed history that supports both 'current price' (where valid_to_date IS NULL) and 'price on day X' (range query). That's the data feeding the anomaly detector's 30-day baselines."*
>
> **中文备注:** Partner 价格大约一周一变,每天爬 4-12 次 → 90% 冗余;SCD-2 只在真变化时插行,带 valid_from/valid_to 区间。"当前价" + "某日价"两种查询都 O(1)。是 anomaly detection 的 30 天基线数据源。

**A+MV 混合:** mv_baseline_staging(MATERIALIZED VIEW)负责计算 → dws_product_price_baseline_1d(物理表)负责持久化 + 去重。

---

## §5 Task B 4 个核心接口详细设计(15 min)

### 5a · `GET /harmonise-product`(3 min)

**指代码:** [`harmonise/`](../Apple%20SDE/harmonise/) 6 个模块。

**核心算法:** 三信号加权评分
```
score = 0.5 × attr_match + 0.3 × token_jaccard + 0.2 × char_fuzz_ratio
```

**结构化 Override:** `attr_match ≥ 0.95` → 强制升 HIGH(避免人工审核过载)。

**Live Demo(在 Swagger UI 演示):**
1. 展开 **`GET /harmonise-product`** → **Try it out**
2. **`q`** 字段填 `iP 17 PM 512GB`,**`k`** 填 `5`,点 **Execute**
3. 响应展示三个信号 breakdown(`attr_match` / `token_jaccard` / `char_fuzz`)+ confidence bucket
4. 再试 `iP15P 128`(没"GB"后缀)→ 还是 HIGH 0.845(走 storage-set 兜底)

**台词:**
> *"No transformer, no embedding. Three signals — structured attribute match dominant at 0.5, token Jaccard 0.3, character SequenceMatcher 0.2 as backup. With 281 reference rows, structured matching is enough; explainability matters more for DQ review than vector similarity."*
>
> **中文备注:** 不用 ML;三信号加权;281 行参考用 ML 是过度工程;DQ 审核需要可解释。

### 5b · `POST /pipeline` + `POST /load-data` 对比(5 min · 重头戏)

**指代码:** [`api/services.py:984`](../Apple%20SDE/api/services.py#L984) `run_pipeline` 朗读 docstring;[`api/services.py:780`](../Apple%20SDE/api/services.py#L780) `submit_load_job` 对比。

**核心架构决策(在 §3 已画过图,这里强调"为什么是这两个端点"):**

1. **`/pipeline` 是 orchestrator**:把 9 个 helper 按 1→2→3→4→5(gate)→6→7→8→9 顺序串起来,**单 PostgreSQL 事务**——任何步骤失败整批回滚。**PRE_FACT 在步 5 之前 gate**,坏行不入 fact。
2. **`/load-data` 是 Task B 字面要求的子模块**:只做 load 相关的步 1+3+5+7+9。题目要求"load data into the table"+ "standardise products using /harmonise-product, country codes, partner names, and timestamps" —— 所以 parse + harmonise + 写 fact 全在这。**不能 gate**(题目要求独立可调,不能等 `/compute-dq` 决定要不要写 fact)。
3. **CTI(Class Table Inheritance)** 表达支付多态 —— `fact_payment_full_price` / `fact_payment_instalment` 子表,父表无 sparse 列。
4. **Slowly Changing Dimension Type 2 历史** —— `fact_partner_price_history` 只在价格真变化时插行(预期 20–50× 行数压缩)。

**台词:**
> *"`/pipeline` and `/load-data` aren't redundant — they live at different abstraction levels. `/pipeline` is the orchestrator: single Postgres transaction, full 9-step interleaved pipeline, PRE_FACT hard gate at step 5 keeps bad rows out of fact. `/load-data` is the Task-B literal sub-module: independently callable, does only the load-related steps (1, 3, 5 with no gate, 7, 9), DQ has to be triggered separately via `/compute-dq`. The trade-off is Path A's stronger guarantees vs Path B's flexibility — same 9 helpers, different orchestration."*

### 5c · `POST /compute-dq`(3 min)

**指代码:** [`api/services.py:853`](../Apple%20SDE/api/services.py#L853) `compute_dq_service`;[`dq/rules.sql`](../Apple%20SDE/dq/rules.sql)(13 条 PL/pgSQL 函数)+ [`dq/rules_split.sql`](../Apple%20SDE/dq/rules_split.sql)(3 个 stage 编排器)。

**Task B 字面要求:** 检查 DQ + 写 `dq_output` + 写 `dq_bad_records`。**已实现且仅做这件事**——request body 只接 `source_batch_id`,内部按 INGEST/PRE_FACT/SEMANTIC 顺序跑全部 13 条,然后聚合返回。

**核心设计:** **DQ 规则全部跑在 PostgreSQL 内**,而不是 Python 逐行检查。
- 每条规则是一个 `STABLE SQL` 函数,返回 `(row_ref, failed_field, error_message, raw_payload)`
- 编排器扫规则目录,按 stage 跑对应规则
- **一次 DB 调用代替 1300 万次 Python 检查**(1M 行 × 13 规则)

**Live Demo(Swagger UI):** 展开 **`POST /compute-dq`** → **Try it out** → Request body 填:
```json
{ "source_batch_id": "<§2c Path B 步骤 1 复制的 BATCH_ID>" }
```
点 **Execute**,响应里能看到每条规则的 pass-rate + by-severity 汇总。

**Live SQL(VSCode 补充):** 跑 [`demo_queries.sql`](demo_queries.sql) §4.1(规则总览)+ §4.2(DQ_HARM_002 拦截示例)展示底层落库情况。

**台词:**
> *"13 rules as PL/pgSQL functions. Postgres vectorises each scan — one DB call replaces 13 M Python checks at 1 M rows. The catalog table is metadata-driven: adding a new rule = one INSERT into `dq_rule_catalog`, no code change. The 3-stage split with severity-driven policy is the architectural innovation; the Task-B-literal contract — `source_batch_id` in, two tables written, summary returned — is what `/compute-dq` exposes."*
>
> **中文备注:** 13 条 SQL 函数;catalog driven 加规则不写代码;3-stage + severity policy 是设计亮点。Request body 只接 source_batch_id,Task B 字面满足。

### 5d · `POST /detect-anomalies`(4 min)

**指代码:** [`api/services.py:691`](../Apple%20SDE/api/services.py#L691) `detect_anomalies` + `_build_anomaly_visualization`。

**4 信号设计** + **每信号独立分类**:

| 信号 | 检查什么 | 实现状态 |
|---|---|:---:|
| STATISTICAL | vs 30 天滚动均值 | ✅ 端到端实现 |
| TEMPORAL | vs 上次有效价 | 🟡 设计中 |
| CROSS_PARTNER | vs 其他 partner 同款 | 🟡 设计中 |
| SKU_VARIANCE | 同 model 跨 SKU 价差 | 🟡 设计中 |

**关键设计:** 每信号触发独立产生 `fact_anomaly` 一行(不合并),便于按 severity 路由到不同团队。

**Visualization payload:**
- `series` — 30 天历史 + 异常点 `is_anomaly: true`
- `baseline_band` — `{mean, lower, upper}` 给前端画基线带
- `cross_partner_comparison` — 其他 partner 当前价

**(可选)Live Demo 注入合成异常:** 跑 [`demo_queries.sql`](demo_queries.sql) §5.1+§5.2,然后 Swagger 调 `POST /detect-anomalies`,展示 visualization 字段填充。**Demo 后** 跑 §5.3+§5.4 清理。

**台词:**
> *"Four signals, each independent — an offer that trips multiple gets multiple `fact_anomaly` rows, each routable to the right team. Visualization payload is structured JSON, not an image — so the same payload feeds Chart.js, a Slack card, or a PDF. All thresholds come from `dim_anomaly_threshold` and are frozen into the response for replay-safety."*
>
> **中文备注:** 4 信号独立分类不合并;visualization 三件套(series + band + cross-partner)结构化输出给前端;阈值 dim_anomaly_threshold 配置化。

---

## §6 Task C 三个回答(6 min)

**对应:** [`presentation.md`](presentation.md) Slide 15–17 / `submission/task_c_answers.md` C.1 + C.2 + C.3。**全节不要 Live Demo,直接走 3 张幻灯片。**

### 6a · Task C-1:新 partner 接入数据模型怎么变(看 Slide 15)

**核心答:** 维度表加行,fact 表不动 —— Kimball 解耦的红利。

| 变更 | 怎么做 | schema 影响 |
|---|---|---|
| 新 partner | `INSERT INTO dim_partner` 一行 | **无** |
| 新支付方式(BNPL) | `ALTER TYPE payment_type_enum` + 新 CTI 子表 | 一个新子表;旧数据零影响 |
| 新国家 | `INSERT INTO dim_country` (+ dim_timezone) | **无** |
| Partner 数据出现未知品类 | `DQ_HARM_002` 抓 → 业务审 → catalog 加 → replay | **无 fact 影响**(rows 永不进 fact) |

**台词:**
> *"Five-row table maps every kind of partner-side change to a dimension-only edit. Class Table Inheritance + harmonise gate make 'fact tables stay stable for years' literally true. New partners onboard via configuration, not migrations."*
>
> **中文备注:** 5 类变更全部映射到维度表的 INSERT/ALTER,fact 表稳定不动。CTI + harmonise gate 让"fact 多年不动"是真的。

### 6b · Task C-2:错误处理 + 业务参与的 DQ 闭环(看 Slide 16)

**核心答:** 三层闭环 — 自动检测 → 业务裁决 → 系统学习。

```
Tier 1 (自动)        Tier 2 (业务)              Tier 3 (学习)
13 DQ rules    ───►  reviewer 看 dq_bad_records  ───►  字典扩、阈值调、新规则
3 stages              raw_payload 完整保留               下批自动变好
                      RESOLVED + replay_batch
                      / IGNORED
```

**关键支撑:** raw_payload JSONB 永不丢、replay 只重跑那一个 source_batch_id、status 字段做 NEW→IN_REVIEW→RESOLVED/IGNORED 工作流。

**台词:**
> *"Three-tier closure. Tier 1 catches everything automatically, Tier 2 gives the business a UI to fix dictionary or thresholds without code, Tier 3 makes the next batch better. The 154-row NZ story we already told in §2 is exactly this loop in action — DQ flagged, business saw raw_payload, fix was a one-line dictionary extension, replay promoted all 154 rows."*
>
> **中文备注:** 三层闭环;NZ 154 行就是这个闭环跑通的真实例子(§2 已演示)。

### 6c · Task C-3:百万级扩展方案(看 Slide 17)

**当前同步流:** 3-7 小时,HTTP 早超时。**5 项改造,3 项已实现:**

| # | 改造 | 状态 |
|---|---|:---:|
| 1 | 异步流水线(HTTP 202 + S3 + ingest_job + 分块 worker pool) | 设计 |
| 2 | PostgreSQL `COPY` 替代 `INSERT`(50–100× 加速) | 设计 |
| 3 | DQ 规则在 SQL 里跑(一次 DB 调用代替 13 M Python 检查) | ✅ 已实现 |
| 4 | 参考数据驻内存(零 per-row DB 查询) | ✅ 部分实现 |
| 5 | 批量 SCD-2(单条 CTE) | ✅ 已实现 |

**性能目标:** 1 M 行 3–7 小时 → **~90 秒**(~200×)。

**台词:**
> *"Three of five performance levers already shipped — the SQL-native DQ, in-memory reference caching, and bulk SCD-2 are all in code today. The remaining two — async pipeline + COPY — are decoupling and infrastructure, not algorithm changes. Migration: swap COPY first (1 day, immediate 50× ingest speedup), then build ingest_job + worker pool (the bigger lift)."*
>
> **中文备注:** 5 个杠杆 3 个已实现;剩两个属于基础设施层;先换 COPY 立刻 50× 加速,再做 worker pool。

---

## §7 反思:挑战 / 亮点 / 可优化(8 min)

### 7a · 主要挑战(3 min)

1. **Schema 反复迭代** —— 一开始按"宽表 + sparse 列"画支付,意识到 sparse 不符合题目要求后改 CTI;DQ 一开始两阶段后扩成三阶段(增加 PRE_FACT gate)
2. **Harmonise 兜底逻辑** —— Partner A 大量"iP15P 128"这种没"GB"后缀的命名,光靠 token 匹配会大量低置信;补 storage-set 兜底 + structural override 后才把 HIGH 占比拉到 86%
3. **测试数据真实性** —— 自己造的样本太干净测不出 DQ 价值;反复用 Partner B 真实 154 行 NZ 数据迭代规则,得出"DQ → 规则迭代 → replay"闭环这个可演示的故事
4. **SCD-2 同日观测边界** —— 重放测试时撞到 valid_from > valid_to 的约束失败,加了"同日守卫"

### 7b · 自认为设计得好的部分(3 min)

1. **三阶段 DQ + Severity 政策**(独创):INGEST/PRE_FACT/SEMANTIC 各自不同失败策略,让 fact 表 by construction 可信,下游不需过滤视图
2. **DQ 全 PL/pgSQL**:19 条规则一次 DB 调用,1M 行 90 秒级而非 30 分钟
3. **可解释的 harmonise**:三信号 breakdown + 结构化 override,业务审核能看懂为什么是这个匹配
4. **Visualization payload 解耦**:返回结构化 JSON 而非渲染好的图,前端 / Slack / PDF 多消费者复用
5. **fact_anomaly 一行一信号**(不合并):支持按 severity 独立路由到不同团队

### 7c · 可继续优化的部分(2 min)

1. **TEMPORAL / CROSS_PARTNER / SKU_VARIANCE 三个 anomaly 信号**:目前只 STATISTICAL 端到端,其他三个 schema 已就位但 detector 待实现
2. **异步管道 + COPY**:C3 里写了设计,demo 跑同步够用;真上百万行需要补这两块基础设施
3. **Harmonise Layer 2**:目前只有 Layer 1(手工字典),没实现 TF-IDF 自动挖掘 Layer 2
4. **Sentence-transformer 备选 score_fn**:作为可插拔接口预留了,没真接入(281 行参考数据现在不需要)
5. **更细粒度的可观测性**:`ingest_job` 表 + Prometheus metrics 还在 C3 设计阶段

**台词:**
> *"What I'd build differently: the async pipeline first, before the algorithms. I prioritised algorithm correctness for the take-home but in production the bottleneck is always I/O, not Python. What I'd keep: the three-stage DQ split with severity policy. That genuinely changes how downstream analytics queries look — no more filter views guarding against unresolved HIGH errors."*

---

## §8 Q&A(30 min)

**应答指南** — 任何"为什么这么设计"问题,直接指对应文件:

| 问题 | 回答位置 |
|---|---|
| 为什么 Class Table Inheritance 表达支付? | `schema.sql` SECTION 2(`payment_type_enum` 上方) |
| 为什么 SCD-2 不是 SCD-1? | `schema.sql` `fact_partner_price_history` 段 |
| 为什么三阶段 DQ? | `task_b_answers.md` B.1 + `task_c_answers.md` C.2 Tier 1 |
| 为什么结构化匹配不用 embedding? | `harmonise/scorer.py` docstring + `task_b_answers.md` B.4 |
| 为什么按月分区? | `schema.sql` partitioning 块 |
| 为什么 FX rate 冻结进 fact 行? | `schema.sql` `fact_price_offer.fx_rate_date` 注释 |
| Partner C 是 JSON 怎么办? | 在 `_step1_csv_to_staging` 加解析器;下游不变 |
| Apple 出新品类怎么办? | `task_c_answers.md` C.1 第 4 行 — Catalog 团队加 Reference + replay |
| 测试覆盖率? | `python3 -m pytest -q` → 44 passed(22 harmonise unit + 22 API integration covering Path A pipeline + 4 Path B 子模块 + path-parity 测试) |
| 为什么有 `/pipeline` 又有 4 个子模块? 重复了吗? | 不重复——共用 9 个 internal helper,零代码重复。`/pipeline` 是单事务硬 gate;子模块独立可调,gate 退化为事后标记。Trade-off。 |
| Path A vs Path B 在数据上能看到差吗? | 能。同份 CSV 跑 Path A:`fact_price_offer` 干净;跑 Path B:fact 含坏行,需 `LEFT JOIN dq_bad_records WHERE bad_record_id IS NULL` 过滤 |
| 怎么衡量 anomaly detection 准确率? | 历史标记数据做 precision/recall;`dim_anomaly_threshold` 支持 A/B 对比 |

---

## 附录 A · 基线状态重置(出乱子时用)

```bash
dropdb maya_assignment 2>/dev/null && createdb maya_assignment
cd "Apple SDE"
psql -d maya_assignment -f schema.sql -f dq/rules.sql -f dq/rules_split.sql
python3 seed_bootstrap.py
# 用 Path A `/pipeline` 一键灌两个 partner(干净 fact,gate 生效)
curl -X POST http://localhost:8000/pipeline -F "file=@Partner A.csv" -F "partner_code=PARTNER_A"
curl -X POST http://localhost:8000/pipeline -F "file=@Partner B.csv" -F "partner_code=PARTNER_B"
```

**预期行数:** stg ~4208 / fact ~4174 / history 119 / bad_records ~188 / dq_output 26(Path A 跑完后)。

## 附录 B · 翻车应急

| 现象 | 修法 |
|---|---|
| 服务崩 | Tab ① `Ctrl+C` → 重启 uvicorn |
| `Errno 48 address already in use` | `lsof -ti:8000 \| xargs kill` |
| Ingest 500 | 看 `/tmp/uvicorn.log`;多半是 DB 约束抓脏数据,讲成 feature |
| Swagger 空白 | `Cmd+Shift+R` 强刷 |
| 时间不够 | 跳 §4d 异常注入,直接讲文档里 visualization 字段定义 |
