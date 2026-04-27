-- ============================================================================
-- schema.sql
-- Product Pricing Offers Data Warehouse 
-- ============================================================================
-- Target Database : PostgreSQL 
-- ============================================================================
--
-- BUSINESS CONTEXT
-- ----------------
-- Scrape, harmonise, and analyse product pricing offers from multiple partner
-- stores (Apple retailers). Supports:
--   * Multi-source reconciliation (different price schemas per partner)
--   * Multi-payment models (full price vs instalment) without sparse columns
--   * Multi-currency, multi-timezone analytics
--   * Data quality monitoring and business-user correction workflow
--   * Pricing anomaly detection
--
-- DESIGN PHILOSOPHY
-- -----------------
-- 1. Kimball star schema — facts record events; dimensions give them meaning.
-- 2. Immutable event facts — every crawl is a new row, never UPDATEd.
-- 3. Class Table Inheritance — payment variants live in child tables so the
--    parent has NO sparse columns.
-- 4. Bi-temporal — business time (crawl_ts_utc) + system time (ingested_at).
-- 5. Timezone-aware — UTC canonical + derived local time for market analytics.
-- 6. Partition-ready — event fact tables partitioned by month for scale.
-- 7. Naming convention — dim_ / fact_ / dws_ / dq_ / app_ / v_ / mv_
--    prefixes, snake_case throughout.
--
-- TABLE TIERS
-- -----------
-- [REQUIRED] Core tables needed for the assignment deliverable.
-- [OPTIONAL] Advanced tables added to support specific stakeholder views
--            (e.g., Finance in Pacific Time, global UTC baselines).
--            Can be skipped in a minimal deployment — storage cost roughly
--            doubles when included.
-- ============================================================================


-- ============================================================================
-- SECTION 0: CUSTOM TYPES
-- ============================================================================
-- ENUMs chosen over strings because:
--   * Type safety — typos rejected at INSERT.
--   * Storage & compare speed vs VARCHAR.
--   * ALTER TYPE required to extend — intentional friction when adding values.

CREATE TYPE payment_type_enum AS ENUM ('FULL', 'INSTALMENT');
-- DESIGN: Discriminator column for Class Table Inheritance.
-- Each value corresponds to one child table under fact_price_offer.
-- Adding a payment method (e.g. BNPL) = ALTER TYPE + new fact_payment_bnpl.

CREATE TYPE harmonise_confidence_enum AS ENUM ('HIGH', 'MEDIUM', 'LOW', 'MANUAL');
-- DESIGN: Score bucketed into interpretable tiers.
-- * HIGH: structured attributes all align (category/storage/chip)
-- * MEDIUM: fuzzy-match fallback above threshold
-- * LOW: below threshold -> routed to dq_bad_records for business review
-- * MANUAL: human-verified override

CREATE TYPE bad_record_status_enum AS ENUM ('NEW', 'IN_REVIEW', 'RESOLVED', 'IGNORED');
-- DESIGN: Workflow state for business-user correction flow (Task C-2).
-- Drives a review UI: NEW -> IN_REVIEW -> RESOLVED / IGNORED.

CREATE TYPE product_lifecycle_enum AS ENUM ('NEW', 'STABLE', 'LEGACY', 'EOL');
-- DESIGN: Drives anomaly-detection sensitivity.
-- * NEW (first 14 d): cross-partner baseline (too little history)
-- * STABLE: default rolling baseline (30 d window, k=2.5)
-- * LEGACY (approaching EOL): widen thresholds, expect strategic price drops
-- * EOL: suppress anomaly alerts entirely


-- ============================================================================
-- SECTION 1: DIMENSION TABLES
-- ============================================================================
-- Dimensions are the "descriptors" of each fact row. They change slowly, are
-- reused across many fact rows, and remain stable when facts onboard new
-- partners, products, or countries — unlike a wide denormalized fact table.


-- ----------------------------------------------------------------------------
-- dim_timezone [REQUIRED]
-- ----------------------------------------------------------------------------
-- Canonical registry of IANA timezones used across the system.
--
-- DESIGN NOTES
-- * Uses IANA names (e.g. 'Australia/Sydney') — NEVER store manual UTC offsets.
--   PG's AT TIME ZONE + system tzdata handle DST transitions and historical
--   rule changes automatically (e.g. when a country abolishes DST).
-- * Decoupled from dim_country because:
--     - One country may span multiple zones (AU has 5, US has 6).
--     - One zone serves multiple countries (Europe/London for UK + IE).
-- ----------------------------------------------------------------------------
CREATE TABLE dim_timezone (
  tz_id         SERIAL       PRIMARY KEY,
  iana_name     VARCHAR(50)  UNIQUE NOT NULL,
  display_name  VARCHAR(100),
  has_dst       BOOLEAN      NOT NULL,
  notes         TEXT
);

INSERT INTO dim_timezone (iana_name, display_name, has_dst, notes) VALUES
  ('UTC',                 'Coordinated Universal Time', FALSE, 'System reference'),
  ('Australia/Sydney',    'Sydney (AEDT/AEST)',         TRUE,  'Primary AU commercial zone'),
  ('Australia/Brisbane',  'Brisbane (AEST)',            FALSE, 'QLD does not observe DST'),
  ('Pacific/Auckland',    'Auckland (NZDT/NZST)',       TRUE,  'NZ primary zone'),
  ('America/Los_Angeles', 'Los Angeles (PST/PDT)',      TRUE,  'Apple HQ + fiscal calendar reference'),
  ('America/New_York',    'New York (EST/EDT)',         TRUE,  'US East Coast'),
  ('Europe/London',       'London (BST/GMT)',           TRUE,  'Typical HQ for global strategy role');


-- ----------------------------------------------------------------------------
-- dim_country [REQUIRED]
-- ----------------------------------------------------------------------------
-- Registry of countries where partners operate. Each country references a
-- primary commercial timezone for market-local date derivation.
--
-- DESIGN NOTES
-- * country_code: ISO 3166-1 alpha-2 (natural business key, human-readable).
-- * primary_tz_id: the single canonical timezone for date bucketing. Multi-
--   zone countries (AU/US) use their main commercial zone. Refine with a
--   per-region timezone table later if needed.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_country (
  country_code   CHAR(2)      PRIMARY KEY,
  country_name   VARCHAR(100) NOT NULL,
  primary_tz_id  INT          NOT NULL REFERENCES dim_timezone(tz_id)
);

INSERT INTO dim_country (country_code, country_name, primary_tz_id) VALUES
  ('AU', 'Australia',      (SELECT tz_id FROM dim_timezone WHERE iana_name = 'Australia/Sydney')),
  ('NZ', 'New Zealand',    (SELECT tz_id FROM dim_timezone WHERE iana_name = 'Pacific/Auckland')),
  ('US', 'United States',  (SELECT tz_id FROM dim_timezone WHERE iana_name = 'America/New_York')),
  ('GB', 'United Kingdom', (SELECT tz_id FROM dim_timezone WHERE iana_name = 'Europe/London'));


-- ----------------------------------------------------------------------------
-- dim_currency [REQUIRED]
-- ----------------------------------------------------------------------------
-- Currency reference. Paired with dim_currency_rate_snapshot for FX lookups.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_currency (
  currency_code  CHAR(3)      PRIMARY KEY,  -- ISO 4217
  currency_name  VARCHAR(100) NOT NULL,
  symbol         VARCHAR(5)
);

INSERT INTO dim_currency (currency_code, currency_name, symbol) VALUES
  ('USD', 'US Dollar',          '$'),
  ('AUD', 'Australian Dollar',  'A$'),
  ('NZD', 'New Zealand Dollar', 'NZ$'),
  ('GBP', 'British Pound',      '£'),
  ('EUR', 'Euro',               '€');


-- ----------------------------------------------------------------------------
-- dim_currency_rate_snapshot [REQUIRED]
-- ----------------------------------------------------------------------------
-- Historical FX rates. Effectively a periodic snapshot (one row per currency
-- pair per day). Although named dim_*, conceptually this straddles the line
-- between dimension and periodic-snapshot fact — Kimball purists would call
-- it fact_currency_rate.
--
-- DESIGN NOTES
-- * Snapshot pattern: (from_ccy, to_ccy, effective_date) uniquely identifies
--   a rate. Rates are temporal truth — never UPDATE, only append corrections
--   with later ingested_at.
-- * Base-currency strategy: store rates only against USD. Derive AUD->NZD as
--   (AUD->USD) / (NZD->USD). Avoids N×N pair explosion.
-- * Forward-fill missing weekend/holiday rates in ETL so every trading day
--   has a row for every active pair.
-- * Consumption: read at load time to freeze fx_rate_to_usd into each
--   fact_price_offer row (see that table's audit columns).
-- ----------------------------------------------------------------------------
CREATE TABLE dim_currency_rate_snapshot (
  rate_id             BIGSERIAL     PRIMARY KEY,
  from_currency_code  CHAR(3)       NOT NULL REFERENCES dim_currency(currency_code),
  to_currency_code    CHAR(3)       NOT NULL REFERENCES dim_currency(currency_code),
  rate                NUMERIC(16,8) NOT NULL CHECK (rate > 0),
  effective_date      DATE          NOT NULL,
  source              VARCHAR(50),            -- 'ECB' / 'OpenExchangeRates' / 'manual'
  ingested_at         TIMESTAMPTZ   DEFAULT NOW(),

  UNIQUE (from_currency_code, to_currency_code, effective_date)
);

CREATE INDEX idx_dim_fx_lookup
  ON dim_currency_rate_snapshot (from_currency_code, effective_date DESC);
-- DESIGN: typical query = "rate for AUD on 2025-10-02" → descending date
-- supports fast most-recent-available lookup with forward fill.


-- ----------------------------------------------------------------------------
-- dim_date [REQUIRED]
-- ----------------------------------------------------------------------------
-- Pre-generated calendar dimension with business-friendly attributes.
--
-- DESIGN NOTES
-- * Preferred over EXTRACT() in every query — consistent, and permits custom
--   fields (fiscal calendar, holidays) not available in ANSI SQL.
-- * Apple fiscal year ends late September; FQ1 = Oct-Dec. Finance users'
--   primary calendar, distinct from calendar-year analysts.
-- * Per-country holiday flags — holidays are local (Boxing Day in AU/NZ,
--   Thanksgiving in US). Extend with more country columns as business grows.
-- * Relative flags (is_current_day/month/year) — maintained by a daily
--   refresh job, simplifies "WHERE is_current_month" queries.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
  date_key               DATE         PRIMARY KEY,
  year                   SMALLINT     NOT NULL,
  quarter                SMALLINT     NOT NULL,
  quarter_name           CHAR(2),
  month                  SMALLINT     NOT NULL,
  month_name             VARCHAR(20),
  month_abbr             CHAR(3),
  week_of_year           SMALLINT,
  day_of_month           SMALLINT,
  day_of_year            SMALLINT,
  day_of_week            SMALLINT,    -- 1 = Monday, 7 = Sunday (ISO)
  day_name               VARCHAR(20),
  is_weekend             BOOLEAN,

  -- Apple fiscal calendar (fiscal year ends late September; FQ1 = Oct-Dec)
  fiscal_year            SMALLINT,
  fiscal_quarter         SMALLINT,
  fiscal_period          SMALLINT,    -- fiscal month (1-12)
  is_fiscal_quarter_end  BOOLEAN,

  -- Per-country holiday flags (extend per country as needed)
  is_holiday_au          BOOLEAN      DEFAULT FALSE,
  holiday_name_au        VARCHAR(100),
  is_holiday_nz          BOOLEAN      DEFAULT FALSE,
  holiday_name_nz        VARCHAR(100),
  is_holiday_us          BOOLEAN      DEFAULT FALSE,
  holiday_name_us        VARCHAR(100),

  -- Refreshed daily by a cron job
  is_current_day         BOOLEAN      DEFAULT FALSE,
  is_current_month       BOOLEAN      DEFAULT FALSE,
  is_current_year        BOOLEAN      DEFAULT FALSE
);

-- Pre-generate 10 years of dates. Fiscal + holiday fields populated by ETL.
INSERT INTO dim_date (
  date_key, year, quarter, quarter_name, month, month_name, month_abbr,
  week_of_year, day_of_month, day_of_year, day_of_week, day_name, is_weekend
)
SELECT
  d::date,
  EXTRACT(year    FROM d)::SMALLINT,
  EXTRACT(quarter FROM d)::SMALLINT,
  'Q' || EXTRACT(quarter FROM d),
  EXTRACT(month   FROM d)::SMALLINT,
  TRIM(TO_CHAR(d, 'Month')),
  TO_CHAR(d, 'Mon'),
  EXTRACT(week    FROM d)::SMALLINT,
  EXTRACT(day     FROM d)::SMALLINT,
  EXTRACT(doy     FROM d)::SMALLINT,
  EXTRACT(isodow  FROM d)::SMALLINT,
  TRIM(TO_CHAR(d, 'Day')),
  EXTRACT(isodow  FROM d) IN (6, 7)
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day') AS d;


-- ----------------------------------------------------------------------------
-- dim_partner [REQUIRED]
-- ----------------------------------------------------------------------------
-- Registry of data sources (partner stores).
--
-- DESIGN NOTES
-- * partner_code: stable, UPPER_SNAKE identifier used in code/config.
-- * Adding a new partner = INSERT one row; fact schema unchanged (payoff of
--   Kimball decoupling).
-- * onboarded_at: audit + SLA analysis ("partners onboarded <30 d ago have
--   noisier data by design").
-- ----------------------------------------------------------------------------
CREATE TABLE dim_partner (
  partner_id    SERIAL       PRIMARY KEY,
  partner_code  VARCHAR(50)  UNIQUE NOT NULL,
  partner_name  VARCHAR(200) NOT NULL,
  country_code  CHAR(2)      REFERENCES dim_country(country_code),
  onboarded_at  TIMESTAMPTZ  DEFAULT NOW(),
  is_active     BOOLEAN      DEFAULT TRUE
);

INSERT INTO dim_partner (partner_code, partner_name, country_code) VALUES
  ('PARTNER_A', 'Partner A', 'AU'),
  ('PARTNER_B', 'Partner B', 'NZ');


-- ----------------------------------------------------------------------------
-- dim_product_category [REQUIRED]
-- ----------------------------------------------------------------------------
-- Product categories carrying category-level attributes that drive downstream
-- logic — NOT just a code-to-name lookup.
--
-- DESIGN NOTES
-- * Snowflake branch off dim_product_model to centralize attributes shared by
--   all models in a category (warranty, return window, lifecycle duration,
--   price volatility class, UI icon).
-- * Each attribute here earns its place by serving a real downstream consumer:
--     - default_warranty_months / default_return_window_days
--         → business logic + customer-facing UI
--     - has_carrier_variants
--         → validation: if FALSE, dim_product_model.connectivity should be NULL
--     - lifecycle_months
--         → used to auto-compute dim_product_model.lifecycle_status transitions
--           (NEW → STABLE after 14d; STABLE → LEGACY at lifecycle_months-3)
--     - price_volatility_class
--         → anomaly detection reads this to pick k_sigma threshold per category
--           (HIGH volatility = wider threshold to avoid alert fatigue)
--     - icon_url
--         → frontend dashboard rendering
-- * If a future category needs a NEW attribute, add a column here rather than
--   duplicating it across every model row.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_product_category (
  category_id                 SERIAL       PRIMARY KEY,
  category_code               VARCHAR(20)  UNIQUE NOT NULL,
  category_name               VARCHAR(100) NOT NULL,
  display_order               INT          DEFAULT 0,

  -- Business-policy attributes
  default_warranty_months     SMALLINT,               -- e.g., 12 for most Apple products
  default_return_window_days  SMALLINT,               -- e.g., 14 in AU/NZ
  has_carrier_variants        BOOLEAN      DEFAULT FALSE, -- TRUE if WiFi/Cellular split applies

  -- Analytics-policy attributes
  lifecycle_months            SMALLINT,               -- typical "current generation" window
  price_volatility_class      VARCHAR(20),            -- 'HIGH' / 'MEDIUM' / 'LOW' — anomaly sensitivity

  -- UI
  icon_url                    TEXT
);

INSERT INTO dim_product_category (
  category_code, category_name, display_order,
  default_warranty_months, default_return_window_days, has_carrier_variants,
  lifecycle_months, price_volatility_class, icon_url
) VALUES
  -- iPhone: new model yearly, carrier variants, medium volatility (launch swings)
  ('IPHONE',  'iPhone',      1, 12, 14, TRUE,  12, 'MEDIUM', '/icons/iphone.svg'),
  -- iPad: refresh ~18mo, carrier variants, low volatility (stable pricing)
  ('IPAD',    'iPad',        2, 12, 14, TRUE,  18, 'LOW',    '/icons/ipad.svg'),
  -- Mac: refresh ~24mo, no carrier, low volatility
  ('MAC',     'Mac',         3, 12, 14, FALSE, 24, 'LOW',    '/icons/mac.svg'),
  -- AirPods: small ticket, high volatility (bundled discounts common)
  ('AIRPODS', 'AirPods',     4, 12, 14, FALSE, 24, 'HIGH',   '/icons/airpods.svg'),
  -- Apple Watch: carrier variants (LTE), medium volatility (seasonal promos)
  ('WATCH',   'Apple Watch', 5, 12, 14, TRUE,  12, 'MEDIUM', '/icons/watch.svg');


-- ----------------------------------------------------------------------------
-- dim_product_model [REQUIRED]
-- ----------------------------------------------------------------------------
-- Harmonised product model — the output target of /harmonise-product API.
--
-- DESIGN NOTES
-- * Double-key pattern:
--     - product_model_id (SERIAL): internal surrogate key for fast JOINs.
--     - model_key (VARCHAR UNIQUE): human-readable business key (e.g.
--       'iphone_17_pro_max_512gb') for idempotent ingestion.
-- * Granularity: one row per "buyable configuration" EXCEPT color. Color is
--   SKU-level (see dim_product_sku) — colors rarely affect price, grouping
--   them yields larger statistical samples for anomaly detection.
-- * Lifecycle fields (launch_date / eol_date / lifecycle_status) feed
--   anomaly detection:
--     - NEW products: cross-partner baseline (not enough history)
--     - LEGACY products: widen thresholds (price drops are expected)
-- * lifecycle_status is AUTO-DERIVED by a daily cron — NOT manually maintained:
--     NEW     : launch_date > CURRENT_DATE - 14 days
--     EOL     : eol_date IS NOT NULL AND eol_date < CURRENT_DATE
--     LEGACY  : launch_date + (category.lifecycle_months - 3) months < CURRENT_DATE
--     STABLE  : otherwise (default on insert)
--   category.lifecycle_months from dim_product_category (iPhone=12, iPad=18,
--   Mac=24) → aligns thresholds with Apple refresh cadence.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_product_model (
  product_model_id  SERIAL       PRIMARY KEY,
  model_key         VARCHAR(200) UNIQUE NOT NULL,
  category_id       INT          NOT NULL REFERENCES dim_product_category(category_id),
  model_line        VARCHAR(100),    -- '17 Pro Max', 'Air (M3)', 'mini (A17 Pro)'
  chip              VARCHAR(50),     -- 'A17 Pro', 'M3', 'M4'
  storage_gb        INT,
  connectivity      VARCHAR(20),     -- 'WiFi' / 'Cellular'

  launch_date       DATE,
  eol_date          DATE,
  lifecycle_status  product_lifecycle_enum DEFAULT 'STABLE',

  created_at        TIMESTAMPTZ  DEFAULT NOW(),
  updated_at        TIMESTAMPTZ  DEFAULT NOW()
);

-- DESIGN: dim_product_model is small (~1k rows at Apple scale). PostgreSQL
-- table scan is sub-millisecond, so an FK index here would cost more than
-- it earns. Add only if EXPLAIN ANALYZE shows category-filter joins as a
-- bottleneck.


-- ----------------------------------------------------------------------------
-- dim_product_sku [REQUIRED]
-- ----------------------------------------------------------------------------
-- SKU-level registry. One product_model has many SKUs (typically one per
-- color variant). Traces back to the Product Ref.csv source.
--
-- DESIGN NOTES
-- * Not used as fact JOIN target — price_offer joins product_model_id because
--   partner price data generally lacks color information.
-- * Preserves long/short descriptions for text search and audit replay.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_product_sku (
  sku_id            SERIAL       PRIMARY KEY,
  product_model_id  INT          NOT NULL REFERENCES dim_product_model(product_model_id),
  ref_product_id    INT,            -- original ID from Product Ref.csv
  color             VARCHAR(50),
  short_desc        TEXT,
  long_desc         TEXT
);

-- DESIGN: same reasoning — dim_product_sku stays in single-table-scan range
-- (~10k rows). Skip the FK index until proven necessary.


-- ----------------------------------------------------------------------------
-- dim_market_event [REQUIRED]
-- ----------------------------------------------------------------------------
-- Known market-moving events (Apple launches, Black Friday, Boxing Day).
--
-- DESIGN NOTES
-- * Consulted by anomaly-detection algorithm to SUPPRESS false positives
--   during known volatility windows (e.g. iPhone 17 launch causes iPhone 15
--   to drop — that's not a data anomaly).
-- * suppression_window_days: symmetric window around event_date; typically
--   7-14 days for product launches, 3-7 for holiday events.
-- * affected_category_code / affected_country_code: NULL = global; otherwise
--   narrow suppression to relevant scope.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_market_event (
  event_id                 SERIAL       PRIMARY KEY,
  event_date               DATE         NOT NULL,
  event_type               VARCHAR(50)  NOT NULL,
  event_name               TEXT,
  affected_country_code    CHAR(2),        -- NULL = global
  affected_category_code   VARCHAR(20),    -- NULL = all categories
  suppression_window_days  SMALLINT     DEFAULT 7
);

-- DESIGN: dim_market_event holds <100 rows (Apple launches + holidays per
-- year). Index would be larger than the data; skip.


-- ----------------------------------------------------------------------------
-- dim_role [REQUIRED]
-- ----------------------------------------------------------------------------
-- Stakeholder role taxonomy. Drives API routing rules in dim_user_preference.
--
-- DESIGN NOTES
-- * Different roles need different default aggregates / timezones:
--     - REGIONAL_SALES → fact_price_offer filtered by country + crawl_date_local
--     - FINANCE        → dws_price_offer_utc_1h re-aggregated to PT [OPTIONAL table]
--     - STRATEGY       → dws_price_offer_utc_1d global comparison [OPTIONAL table]
--   Roles without an OPTIONAL table available fall back to ad-hoc queries on
--   fact_price_offer + its indexes.
-- * Separating role from user enables role-based permissions later.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_role (
  role_code    VARCHAR(50)  PRIMARY KEY,
  role_name    VARCHAR(100) NOT NULL,
  description  TEXT
);

INSERT INTO dim_role (role_code, role_name, description) VALUES
  ('STRATEGY',       'Pricing Strategy',     'Global pricing decisions, typically HQ-based'),
  ('REGIONAL_SALES', 'Regional Sales',       'Country-specific partner pricing monitoring'),
  ('FINANCE',        'Finance & Accounting', 'Fiscal-calendar reporting (Apple FY)'),
  ('ANALYST',        'Data Analyst',         'Ad-hoc cross-domain analysis'),
  ('EXECUTIVE',      'Executive',            'High-level KPI dashboards');


-- ----------------------------------------------------------------------------
-- dim_anomaly_threshold [REQUIRED]
-- ----------------------------------------------------------------------------
-- Centralized configuration table for ALL tunable parameters in the anomaly
-- detection algorithm. Nothing in the detection code should hardcode a magic
-- number — every threshold / weight / factor is looked up from this table.
--
-- DESIGN NOTES
-- * Every placeholder value in the code is initially sourced from here, with
--   `source = 'initial_guess'` and a `rationale` explaining what it means.
-- * Values can be overridden per category and/or per country (more specific
--   rows win). The helper function get_anomaly_threshold() below resolves the
--   most specific value available.
-- * Every change should set `last_reviewed_at` and `updated_by` — audit trail
--   that lets business justify why a threshold was tuned.
--
-- CALIBRATION PLAN PER KEY
-- * signal_weight_*         → Logistic Regression over labeled anomalies
-- * severity_bar_*          → percentile of final_score sized to team capacity
-- * cross_partner_*         → percentiles of historical cross-partner spreads
-- * temporal_*              → percentiles of historical daily price changes
-- * lifecycle_factor_*      → held constant until calibrated via A/B testing
-- * event_suppression_*     → A/B tested against labeled events
-- * category_sensitivity_*  → derived from per-category stddev/mean ratios
-- ----------------------------------------------------------------------------
CREATE TABLE dim_anomaly_threshold (
  threshold_id      SERIAL        PRIMARY KEY,
  config_key        VARCHAR(80)   NOT NULL,
  config_value      NUMERIC(12,4) NOT NULL,
  config_type       VARCHAR(30),         -- 'weight' / 'threshold' / 'factor' / 'percentile'

  -- Scope (NULL = global default; more specific row wins)
  category_code     VARCHAR(20)   REFERENCES dim_product_category(category_code),
  country_code      CHAR(2)       REFERENCES dim_country(country_code),

  -- Audit / governance
  rationale         TEXT          NOT NULL,
  source            VARCHAR(30)   NOT NULL,   -- 'initial_guess' / 'data_calibrated' / 'business_agreed'
  last_reviewed_at  DATE          DEFAULT CURRENT_DATE,
  updated_by        VARCHAR(100),

  UNIQUE (config_key, category_code, country_code)
);

-- Most-specific-wins lookup helper (used by /detect-anomalies backend)
CREATE OR REPLACE FUNCTION get_anomaly_threshold(
  p_key      VARCHAR,
  p_category VARCHAR DEFAULT NULL,
  p_country  CHAR(2) DEFAULT NULL
) RETURNS NUMERIC AS $$
  SELECT config_value FROM dim_anomaly_threshold
  WHERE config_key = p_key
    AND (category_code = p_category OR category_code IS NULL)
    AND (country_code  = p_country  OR country_code  IS NULL)
  ORDER BY
    (category_code IS NOT NULL)::int DESC,   -- category match first
    (country_code  IS NOT NULL)::int DESC    -- then country match
  LIMIT 1;
$$ LANGUAGE SQL STABLE;

-- Seed: initial guesses (all flagged as such in 'source')
INSERT INTO dim_anomaly_threshold (config_key, config_value, config_type, rationale, source) VALUES
  ('signal_weight_statistical',    0.40, 'weight',    'Initial guess — calibrate via logistic regression on labeled anomalies', 'initial_guess'),
  ('signal_weight_temporal',       0.30, 'weight',    'Initial guess — calibrate via logistic regression', 'initial_guess'),
  ('signal_weight_cross_partner',  0.30, 'weight',    'Initial guess — calibrate via logistic regression', 'initial_guess'),

  ('severity_bar_high',            0.80, 'threshold', 'HIGH cutoff — re-calibrate to p95 of final_score so ~5% of detections escalate', 'initial_guess'),
  ('severity_bar_medium',          0.50, 'threshold', 'MEDIUM cutoff — re-calibrate to p80', 'initial_guess'),
  ('severity_bar_low',             0.25, 'threshold', 'LOW cutoff — below = not recorded', 'initial_guess'),

  ('cross_partner_suspicious_pct', 0.15, 'threshold', 'Initial guess — calibrate via p90 of historical cross-partner spread', 'initial_guess'),
  ('cross_partner_severe_pct',     0.25, 'threshold', 'Initial guess — calibrate via p99', 'initial_guess'),

  ('temporal_mild_pct',            0.05, 'threshold', 'Initial guess — below = no signal; calibrate via p50 of DoD changes', 'initial_guess'),
  ('temporal_suspicious_pct',      0.10, 'threshold', 'Initial guess — calibrate via p90', 'initial_guess'),
  ('temporal_severe_pct',          0.20, 'threshold', 'Initial guess — calibrate via p95', 'initial_guess'),
  ('temporal_breach_pct',          0.30, 'threshold', 'Initial guess — calibrate via p99', 'initial_guess'),

  ('lifecycle_factor_NEW',         0.30, 'factor',    'Initial guess — NEW products have unstable baselines, reduce sensitivity', 'initial_guess'),
  ('lifecycle_factor_STABLE',      1.00, 'factor',    'Default — no adjustment', 'initial_guess'),
  ('lifecycle_factor_LEGACY',      0.60, 'factor',    'Initial guess — legacy products naturally drop in price', 'initial_guess'),
  ('lifecycle_factor_EOL',         0.00, 'factor',    'EOL products suppressed entirely — no anomaly alerts', 'business_agreed'),

  ('event_suppression_APPLE_LAUNCH', 0.40, 'factor',  'Initial guess — A/B test against labeled launch events', 'initial_guess'),
  ('event_suppression_BLACK_FRIDAY', 0.50, 'factor',  'Initial guess', 'initial_guess'),
  ('event_suppression_BOXING_DAY',   0.50, 'factor',  'Initial guess', 'initial_guess'),

  ('category_sensitivity_LOW',     1.00, 'factor',    'LOW volatility category — default k_sigma (e.g., Mac)', 'initial_guess'),
  ('category_sensitivity_MEDIUM',  1.30, 'factor',    'MEDIUM volatility (e.g., iPhone)', 'initial_guess'),
  ('category_sensitivity_HIGH',    1.80, 'factor',    'HIGH volatility (e.g., AirPods — promo-heavy)', 'initial_guess'),

  ('sku_variance_threshold_pct',   0.03, 'threshold', 'Max intra-partner intra-product price spread before SKU_VARIANCE anomaly fires', 'initial_guess'),

  ('min_baseline_sample_size',     14,   'threshold', 'Baselines with sample_size < this are unreliable — fallback to cross-partner baseline', 'initial_guess');


-- ----------------------------------------------------------------------------
-- dim_alert_channel [REQUIRED]
-- ----------------------------------------------------------------------------
-- Registry of notification channels the alerting system can emit to.
--
-- DESIGN NOTES
-- * Only channels appropriate for this workload: Slack / Teams / Email / Webhook.
-- * PHONE and SMS intentionally excluded — this is a non-customer-facing
--   analysis pipeline; hour-level latency is acceptable; avoiding alert
--   fatigue is more important than instant notification.
-- * config_json holds channel-specific setup (webhook URL, SMTP server etc.)
-- ----------------------------------------------------------------------------
CREATE TABLE dim_alert_channel (
  channel_id     SERIAL        PRIMARY KEY,
  channel_code   VARCHAR(30)   UNIQUE NOT NULL,   -- 'SLACK' / 'TEAMS' / 'EMAIL' / 'WEBHOOK'
  channel_name   VARCHAR(100)  NOT NULL,
  config_json    JSONB,                           -- webhook URL, API keys, etc.
  is_active      BOOLEAN       DEFAULT TRUE,
  created_at     TIMESTAMPTZ   DEFAULT NOW()
);

INSERT INTO dim_alert_channel (channel_code, channel_name, is_active) VALUES
  ('SLACK',   'Slack workspace',       TRUE),
  ('TEAMS',   'Microsoft Teams',       TRUE),
  ('EMAIL',   'Email (daily digest)',  TRUE),
  ('WEBHOOK', 'Generic webhook (Jira/Linear ticket creation)', TRUE);


-- ----------------------------------------------------------------------------
-- dim_alert_policy [REQUIRED]
-- ----------------------------------------------------------------------------
-- Routing rules: which anomalies go to which channel/recipients.
--
-- DESIGN NOTES
-- * Matching logic: a policy applies if severity >= min_severity AND
--   (anomaly_type matches OR policy.anomaly_type IS NULL) AND similar for
--   country/category.
-- * One anomaly can fire multiple policies (e.g., Slack + email digest).
-- * Quiet hours avoid alerting at inconvenient local times.
-- * recipient_type distinguishes individual vs group vs oncall rotation —
--   useful if integrated with a PagerDuty-like system (future).
-- ----------------------------------------------------------------------------
CREATE TABLE dim_alert_policy (
  policy_id          SERIAL       PRIMARY KEY,
  policy_name        VARCHAR(100) NOT NULL,
  min_severity       VARCHAR(10)  NOT NULL,     -- 'HIGH' / 'MEDIUM' / 'LOW'
  anomaly_type       VARCHAR(30),                -- NULL = any type
  country_code       CHAR(2)      REFERENCES dim_country(country_code),
  category_code      VARCHAR(20)  REFERENCES dim_product_category(category_code),
  channel_id         INT          REFERENCES dim_alert_channel(channel_id),
  recipient          VARCHAR(200),               -- email / Slack channel / webhook URL
  recipient_type     VARCHAR(20),                -- 'INDIVIDUAL' / 'GROUP' / 'ONCALL_ROTATION'
  quiet_hours_start  TIME,
  quiet_hours_end    TIME,
  digest_send_time   TIME,                       -- for EMAIL digest policies
  is_active          BOOLEAN      DEFAULT TRUE,
  created_at         TIMESTAMPTZ  DEFAULT NOW()
);

-- DESIGN: dim_alert_policy holds tens of rules at most. Skip indexing.


-- ============================================================================
-- SECTION 2: FACT TABLES — EVENT GRAIN
-- ============================================================================


-- ----------------------------------------------------------------------------
-- fact_price_offer [REQUIRED]
-- ----------------------------------------------------------------------------
-- Immutable event-level fact: one row per pricing observation from a partner.
--
-- DESIGN NOTES
-- * IMMUTABILITY: never UPDATE, only INSERT. Corrections = new rows with
--   later ingested_at.
-- * BI-TEMPORAL tracking:
--     - crawl_ts_utc    : when the price was observed (event/business time)
--     - ingested_at     : when we loaded it (system time)
--   Separating the two enables late-arrival handling and replay debugging.
-- * TIMEZONE HANDLING:
--     - crawl_ts_utc    : canonical UTC (TIMESTAMPTZ)
--     - crawl_ts_local  : partner-country local time (TIMESTAMP, no TZ)
--     - crawl_date_local: market-local date, GENERATED STORED, joins dim_date
--   Enables both "global UTC" view (Tier 2 aggregates) and "market-local"
--   view (Tier 1 aggregates) without runtime recomputation.
-- * CURRENCY DENORMALIZATION — frozen snapshot:
--     Stores local amount, USD amount, AND the fx_rate used + fx_rate_date.
--     Rationale:
--       - O(1) analytical queries (no JOIN to rate table)
--       - Full audit trail ("what rate did we use?")
--       - Immutability (even if rate table is corrected later, historical
--         facts keep their original USD value)
--       - Recomputability (store enough to redo the calculation)
-- * HARMONISE PROVENANCE: score + confidence stored per row, so DQ queries
--   flag LOW-confidence matches without re-running the harmonise algorithm.
-- * PARTITIONING: monthly RANGE partition on crawl_ts_utc. Monthly is
--   chosen for these reasons (in priority order):
--     1. PRIMARY — Pricing data updates infrequently (partners change prices
--        ~weekly), so daily-grained partitions would be overkill. Monthly
--        granularity matches both the data update frequency AND the typical
--        analytical query window ("this month", "this quarter", "last 30d").
--     2. Aligns with Apple fiscal months (Finance queries naturally prune)
--     3. Launch-month spikes (Sep for iPhone) are handled at the algorithm
--        layer (dim_market_event suppression windows), NOT at the partition
--        layer — keeps the physical schema simple and stable.
-- * CLASS TABLE INHERITANCE: this is the parent; payment specifics live in
--   fact_payment_full_price and fact_payment_instalment so the parent has
--   no NULLable payment columns (non-sparse design, explicit requirement).
-- ----------------------------------------------------------------------------
CREATE TABLE fact_price_offer (
  offer_id                 BIGSERIAL   NOT NULL,

  -- Dimension FKs
  partner_id               INT         NOT NULL REFERENCES dim_partner(partner_id),
  country_code             CHAR(2)     NOT NULL REFERENCES dim_country(country_code),
  product_model_id         INT         REFERENCES dim_product_model(product_model_id),
  sku_id                   INT         REFERENCES dim_product_sku(sku_id),
  -- DESIGN: sku_id is OPTIONAL. Populated when partner data distinguishes
  -- colors / carrier variants (e.g., Partner B's "White Titanium"). When
  -- present, enables the SKU_VARIANCE anomaly type — detecting suspicious
  -- intra-product price spreads within one partner. NULL when partner data
  -- gives only model-level info.

  -- Business payload
  raw_product_name         TEXT        NOT NULL,          -- preserved for audit
  payment_type             payment_type_enum NOT NULL,    -- CTI discriminator

  -- Money: original + normalized + audit snapshot
  currency_code            CHAR(3)     NOT NULL REFERENCES dim_currency(currency_code),
  effective_total_local    NUMERIC(12,2) NOT NULL CHECK (effective_total_local > 0),
  effective_total_usd      NUMERIC(12,2) NOT NULL CHECK (effective_total_usd > 0),
  fx_rate_to_usd           NUMERIC(16,8) NOT NULL,
  fx_rate_date             DATE        NOT NULL,
  fx_rate_source           VARCHAR(50),

  -- Temporal (bi-temporal + timezone-aware)
  crawl_ts_utc             TIMESTAMPTZ NOT NULL,          -- business time (UTC)
  crawl_ts_local           TIMESTAMP   NOT NULL,          -- market-local
  crawl_date_local         DATE        GENERATED ALWAYS AS (crawl_ts_local::date) STORED,
  ingested_at              TIMESTAMPTZ DEFAULT NOW(),     -- system time

  -- Harmonise provenance
  harmonise_score          NUMERIC(4,3),
  harmonise_confidence     harmonise_confidence_enum,

  -- Batch tracking (idempotency + replay)
  source_batch_id          UUID,

  PRIMARY KEY (offer_id, crawl_ts_utc)    -- partition key must be in PK
) PARTITION BY RANGE (crawl_ts_utc);

-- ----------------------------------------------------------------------------
-- PARTITIONING STRATEGY
-- ----------------------------------------------------------------------------
-- The parent table above declares PARTITION BY RANGE (crawl_ts_utc). Data is
-- physically stored in per-month child tables. Benefits over a single big
-- table + WHERE filter:
--   * Partition pruning: planner skips irrelevant months at planning time
--   * DROP PARTITION for instant archival (vs slow DELETE + VACUUM)
--   * Per-partition VACUUM / REINDEX — no global lock
--   * Write lock isolation: concurrent loads for different months don't
--     contend on the same table
--
-- DEMO setup: a single DEFAULT partition. Since PostgreSQL requires every
-- partitioned table to have at least one valid target for inserts, the
-- default partition acts as the universal landing zone for all rows.
--
-- In production we'd switch to pg_partman (commented block below). It would
-- detach the rows from the default partition into auto-generated monthly
-- partitions on a schedule, giving us per-month query pruning + archival
-- without manual DDL maintenance.
-- ----------------------------------------------------------------------------

CREATE TABLE fact_price_offer_default PARTITION OF fact_price_offer DEFAULT;
-- DESIGN: single default partition keeps the GUI clean and the demo simple.
-- Every row lands here; queries on the parent table still work the same way.
-- Production replaces this with pg_partman-managed monthly partitions.

-- (2) PRODUCTION — pg_partman automated monthly partition management
-- ----------------------------------------------------------------------------
-- Uncomment after installing the extension:
--   CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
--
-- One-time setup (run as superuser):
/*
SELECT partman.create_parent(
  p_parent_table => 'public.fact_price_offer',
  p_control      => 'crawl_ts_utc',        -- partition key column
  p_type         => 'range',
  p_interval     => '1 month',
  p_premake      => 6                       -- create next 6 months in advance
);

-- Retention: auto-drop partitions older than 24 months
UPDATE partman.part_config
SET retention                 = '24 months',
    retention_keep_table      = false,     -- true = keep detached; false = DROP
    infinite_time_partitions  = true
WHERE parent_table = 'public.fact_price_offer';
*/

-- Daily maintenance (run via cron / pg_cron / Airflow):
/*
SELECT partman.run_maintenance();
-- Creates upcoming partitions + archives/drops expired ones per retention rule.
*/
-- ----------------------------------------------------------------------------

-- Indexes — defined on parent, inherited by all partitions
CREATE INDEX idx_offer_model_ts
  ON fact_price_offer(product_model_id, crawl_ts_utc);
-- DESIGN: most common query = "price history for product X over time range".
-- Equality leading column, range trailing — optimal B-tree layout.

CREATE INDEX idx_offer_partner_ts
  ON fact_price_offer(partner_id, crawl_ts_utc);
-- DESIGN: partner-level audit ("all Partner A offers on day X") and DQ checks.

CREATE INDEX idx_offer_country_date_local
  ON fact_price_offer(country_code, crawl_date_local);
-- DESIGN: Regional Sales dashboards ("AU yesterday") bypass TZ conversion.

CREATE INDEX idx_offer_low_conf
  ON fact_price_offer(harmonise_confidence, ingested_at)
  WHERE harmonise_confidence IN ('LOW', 'MEDIUM');
-- DESIGN: partial index — only covers offers needing review.
-- Keeps DQ inbox queries fast without bloating the main index.


-- ----------------------------------------------------------------------------
-- fact_payment_full_price [REQUIRED]
-- ----------------------------------------------------------------------------
-- Child table: FULL payment variant. 1:1 with fact_price_offer rows where
-- payment_type = 'FULL'.
--
-- DESIGN NOTES
-- * Class Table Inheritance payoff: full_price has a real NOT NULL + CHECK
--   constraint (impossible in a sparse single-table design where the column
--   would need to be NULLable to accommodate instalment rows).
-- * Mirrors the parent's partition key so DROP PARTITION cascades cleanly
--   across parent + child when archiving.
-- ----------------------------------------------------------------------------
CREATE TABLE fact_payment_full_price (
  offer_id      BIGINT        NOT NULL,
  crawl_ts_utc  TIMESTAMPTZ   NOT NULL,                          -- partition mirror
  full_price    NUMERIC(12,2) NOT NULL CHECK (full_price > 0),

  PRIMARY KEY (offer_id, crawl_ts_utc),
  FOREIGN KEY (offer_id, crawl_ts_utc)
    REFERENCES fact_price_offer(offer_id, crawl_ts_utc)
) PARTITION BY RANGE (crawl_ts_utc);

-- Single DEFAULT partition — see fact_price_offer above for full rationale
CREATE TABLE fact_payment_full_price_default PARTITION OF fact_payment_full_price DEFAULT;


-- ----------------------------------------------------------------------------
-- fact_payment_instalment [REQUIRED]
-- ----------------------------------------------------------------------------
-- Child table: INSTALMENT payment variant. 1:1 with fact_price_offer rows
-- where payment_type = 'INSTALMENT'.
--
-- DESIGN NOTES
-- * effective_total_local in the parent = monthly_amount × instalment_months.
--   This normalization is done at load time so downstream queries (anomaly
--   detection, aggregates) can treat FULL and INSTALMENT offers uniformly.
-- * CHECK (instalment_months BETWEEN 1 AND 60) catches common bad data:
--   negative months, zero, or absurd values — observed in raw partner feeds.
-- ----------------------------------------------------------------------------
CREATE TABLE fact_payment_instalment (
  offer_id           BIGINT        NOT NULL,
  crawl_ts_utc       TIMESTAMPTZ   NOT NULL,
  monthly_amount     NUMERIC(12,2) NOT NULL CHECK (monthly_amount > 0),
  instalment_months  SMALLINT      NOT NULL CHECK (instalment_months BETWEEN 1 AND 60),

  PRIMARY KEY (offer_id, crawl_ts_utc),
  FOREIGN KEY (offer_id, crawl_ts_utc)
    REFERENCES fact_price_offer(offer_id, crawl_ts_utc)
) PARTITION BY RANGE (crawl_ts_utc);

-- Single DEFAULT partition — see fact_price_offer above for full rationale
CREATE TABLE fact_payment_instalment_default PARTITION OF fact_payment_instalment DEFAULT;


-- ----------------------------------------------------------------------------
-- fact_partner_price_history [REQUIRED]
-- ----------------------------------------------------------------------------
-- Compressed price history using Slowly Changing Dimension Type 2 pattern.
-- Only inserts a new row when the price actually changes — enormous
-- storage saving vs fact_price_offer which records every crawl observation.
--
-- DESIGN NOTES
-- * Source: derived from fact_price_offer by incremental ETL.
-- * Change-detection logic (runs after each load batch):
--     For each unique (product, partner, country, payment_type):
--       Find most recent history row (valid_to_date IS NULL).
--       Compare its price with latest observation from fact_price_offer.
--       IF price same → skip (no new row).
--       IF price different:
--         (1) UPDATE existing row SET valid_to_date = new.crawl_date_local - 1
--         (2) INSERT new row with valid_from_date = new.crawl_date_local,
--             valid_to_date = NULL
-- * Expected compression: Partner prices typically change ~weekly, not daily.
--   For 30-day crawl history, expect 20-50× row reduction vs fact_price_offer.
-- * Query "price on date X":
--     WHERE valid_from_date <= X
--       AND (valid_to_date IS NULL OR valid_to_date >= X)
-- * Consumers:
--     - v_partner_price_current (the current-snapshot VIEW below)
--     - mv_baseline_staging (computes rolling baselines)
--     - Any "price on date X" query
-- * NOT partitioned — change-compressed data stays small even for years of
--   history (total rows = products × partners × countries × avg change count).
-- ----------------------------------------------------------------------------
CREATE TABLE fact_partner_price_history (
  history_id             BIGSERIAL    PRIMARY KEY,

  product_model_id       INT          NOT NULL REFERENCES dim_product_model(product_model_id),
  partner_id             INT          NOT NULL REFERENCES dim_partner(partner_id),
  country_code           CHAR(2)      NOT NULL REFERENCES dim_country(country_code),
  payment_type           payment_type_enum NOT NULL,
  sku_id                 INT          REFERENCES dim_product_sku(sku_id),
  -- DESIGN: sku_id mirrors fact_price_offer. When partner data is SKU-aware,
  -- history is tracked at SKU granularity so color-specific price changes are
  -- captured separately. Default (NULL) is model-level history.

  currency_code          CHAR(3)      NOT NULL REFERENCES dim_currency(currency_code),
  effective_total_local  NUMERIC(12,2) NOT NULL CHECK (effective_total_local > 0),
  effective_total_usd    NUMERIC(12,2) NOT NULL CHECK (effective_total_usd > 0),

  valid_from_date        DATE         NOT NULL,
  valid_to_date          DATE,                              -- NULL = currently active

  created_at             TIMESTAMPTZ  DEFAULT NOW(),

  -- sku_id included in uniqueness: same product_model with different SKUs
  -- can have independent price histories
  UNIQUE (product_model_id, partner_id, country_code, payment_type, sku_id, valid_from_date),
  CHECK (valid_to_date IS NULL OR valid_to_date >= valid_from_date)
);

-- Partial index: only current-active rows, used by v_partner_price_current.
-- Tiny and fast because most history rows are historical (not current).
CREATE INDEX idx_price_history_current
  ON fact_partner_price_history (product_model_id, country_code)
  WHERE valid_to_date IS NULL;

-- Range query index: "price of product X in country C on date D".
-- Leading equality columns + range columns — optimal for range lookups.
CREATE INDEX idx_price_history_date_range
  ON fact_partner_price_history (product_model_id, country_code,
                                 valid_from_date, valid_to_date);


-- ----------------------------------------------------------------------------
-- fact_anomaly [REQUIRED]
-- ----------------------------------------------------------------------------
-- Detected anomalies from /detect-anomalies — the audit-ready record of every
-- anomaly the system has identified.
--
-- KEY DESIGN: ONE ROW PER TRIGGERED SIGNAL (not per offer)
-- ---------------------------------------------------------
-- Each triggered signal (STATISTICAL / TEMPORAL / CROSS_PARTNER /
-- SKU_VARIANCE) produces its OWN row with its OWN severity. If one offer
-- trips multiple signals, multiple rows are emitted.
--
-- Rationale: a single offer_id with signals {A=OK, B=HIGH, C=OK} combined
-- into ONE composite severity would dilute B's seriousness, or worse, hide
-- it entirely. Each concern deserves independent routing and resolution
-- (e.g., TEMPORAL → operations; CROSS_PARTNER → pricing strategy).
--
-- OTHER DESIGN NOTES
-- * threshold_snapshot JSONB freezes the exact thresholds used at detection
--   time. Even if dim_anomaly_threshold is later updated, past alerts can be
--   reproduced exactly (replay proof).
-- * signal_score is this signal's independent 0-1 score (NOT a composite).
-- * Context factors (lifecycle_factor, event_suppression_factor, category
--   _sensitivity) are stored per-row so tooling can explain "why HIGH but
--   suppressed" transparently.
-- * Workflow fields (status/assignee/resolution_notes) mirror dq_bad_records
--   — same review UX.
-- * suppression_event_id → dim_market_event links the row to the event that
--   dampened its score (if any). Powerful for post-mortem "why didn't we
--   alert on this?"
-- * UNIQUE (offer_id, anomaly_type): don't produce duplicate anomaly rows
--   for the same offer + same signal type.
-- ----------------------------------------------------------------------------
CREATE TABLE fact_anomaly (
  anomaly_id                BIGSERIAL     PRIMARY KEY,

  -- Anchoring
  offer_id                  BIGINT        NOT NULL,
  crawl_ts_utc              TIMESTAMPTZ   NOT NULL,
  source_batch_id           UUID,

  -- Context dimensions (denormalized for fast filtering)
  product_model_id          INT           REFERENCES dim_product_model(product_model_id),
  partner_id                INT           REFERENCES dim_partner(partner_id),
  country_code              CHAR(2)       REFERENCES dim_country(country_code),

  -- What this row represents
  anomaly_type              VARCHAR(30)   NOT NULL,
    -- STATISTICAL / TEMPORAL / CROSS_PARTNER / SKU_VARIANCE
    -- NOT 'COMPOSITE' — each row = one signal, independent classification
  signal_score              NUMERIC(4,3)  NOT NULL,    -- this signal's own 0-1 score
  severity                  VARCHAR(10)   NOT NULL,    -- HIGH / MEDIUM / LOW (from dim_anomaly_threshold.severity_bar_*)

  -- Contextual adjustment factors applied
  lifecycle_factor          NUMERIC(4,2),
  event_suppression_factor  NUMERIC(4,2),
  category_sensitivity      NUMERIC(4,2),

  -- Observation + reference values (denormalized for self-contained row)
  observed_price_usd        NUMERIC(12,2),
  baseline_snapshot         JSONB,
    -- e.g., {"mean": 1870, "stddev": 45, "p05": 1799, "p95": 1920, "sample_size": 28, "window_days": 30}
  threshold_snapshot        JSONB,
    -- e.g., {"severity_bar_high": 0.80, "signal_weight_temporal": 0.30, ...}
    -- Freezes dim_anomaly_threshold values at detection time → replay-safe

  -- Suppression tracing (why was this row's severity dampened?)
  suppression_applied       BOOLEAN       DEFAULT FALSE,
  suppression_event_id      INT           REFERENCES dim_market_event(event_id),

  -- Workflow (mirror of dq_bad_records pattern)
  status                    VARCHAR(20)   DEFAULT 'NEW',   -- NEW / ACKNOWLEDGED / RESOLVED / FALSE_POSITIVE
  assignee                  VARCHAR(100),
  resolution_notes          TEXT,

  detected_at               TIMESTAMPTZ   DEFAULT NOW(),
  resolved_at               TIMESTAMPTZ,

  UNIQUE (offer_id, anomaly_type)
);

CREATE INDEX idx_anomaly_severity_time ON fact_anomaly(severity, detected_at DESC);
CREATE INDEX idx_anomaly_batch         ON fact_anomaly(source_batch_id);
CREATE INDEX idx_anomaly_status_sev
  ON fact_anomaly(status, severity)
  WHERE status IN ('NEW','ACKNOWLEDGED');
-- DESIGN: partial index supports the primary workflow query
-- "show me HIGH severity anomalies still awaiting review".


-- ============================================================================
-- SECTION 3: SUMMARY TABLES (DWS LAYER)
--   Naming: dws_<subject>_<variant>_<period>
--   Period suffixes: _1d (daily) / _1m (monthly) / _30d (30-day rolling) /
--                    _1h (hourly) / _td (to-date cumulative)
-- ============================================================================
-- Rolled-up derivatives of fact_price_offer, refreshed by ETL jobs.
-- Separating aggregates from the event table keeps the event table optimized
-- for writes and allows aggregates to be rebuilt without affecting raw data.


-- ----------------------------------------------------------------------------
-- NOTE: earlier design iterations included dws_price_offer_market_local_1d
-- and dws_price_offer_td, but they were removed on review: no API endpoint
-- in the assignment depends on them, and at current scale the same queries
-- can be served directly off fact_price_offer with its (country, date_local)
-- index. If Regional Sales dashboards or portfolio-overview views are added
-- later, reintroduce them as MATERIALIZED VIEWs first, then promote to full
-- tables only if refresh latency becomes an issue.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- dws_price_offer_utc_1d [OPTIONAL]
-- ----------------------------------------------------------------------------
-- Daily snapshot aggregated at UTC date. Same grain as _1d_market_local but
-- using UTC day boundaries.
--
-- WHY OPTIONAL
-- * Duplicates data from _1d_market_local (~same size).
-- * Only needed for specific use cases:
--     - Anomaly detection baselines (TZ-invariant grain for algorithm
--       stability)
--     - Global cross-country comparison on "same UTC day"
-- * Can be skipped if storage is constrained — data science consumers can
--   rebuild on demand via hourly table (also optional) or the event fact.
--
-- DESIGN NOTES
-- * Schema mirrors _1d_market_local — only the date field semantics differ.
-- * Refreshed concurrently with _1d_market_local from the same source.
-- ----------------------------------------------------------------------------
CREATE TABLE dws_price_offer_utc_1d (
  utc_date          DATE         NOT NULL,
  partner_id        INT          NOT NULL REFERENCES dim_partner(partner_id),
  country_code      CHAR(2)      NOT NULL REFERENCES dim_country(country_code),
  product_model_id  INT          NOT NULL REFERENCES dim_product_model(product_model_id),
  payment_type      payment_type_enum NOT NULL,

  offer_count       INT          NOT NULL,
  min_price_usd     NUMERIC(12,2),
  avg_price_usd     NUMERIC(12,2),
  median_price_usd  NUMERIC(12,2),
  max_price_usd     NUMERIC(12,2),
  stddev_price_usd  NUMERIC(12,2),

  refreshed_at      TIMESTAMPTZ  DEFAULT NOW(),

  PRIMARY KEY (utc_date, partner_id, country_code, product_model_id, payment_type)
) PARTITION BY RANGE (utc_date);


-- ----------------------------------------------------------------------------
-- dws_price_offer_utc_1h [OPTIONAL]
-- ----------------------------------------------------------------------------
-- Hourly grain aggregation. Enables dynamic re-aggregation into any timezone
-- at query time (e.g. Finance team wants Los Angeles fiscal-day rollup).
--
-- WHY OPTIONAL
-- * 24× row count of the daily aggregate. Typically maintained only for the
--   last ~90 days (hot data window).
-- * Only valuable if stakeholders need rollups that match neither UTC day
--   nor market-local day (e.g. HQ in London doing Pacific Time fiscal view).
--
-- DESIGN NOTES
-- * utc_hour truncated to hour precision (TIMESTAMP, interpreted as UTC).
-- * Query pattern example:
--     SELECT DATE_TRUNC('day', utc_hour AT TIME ZONE 'America/Los_Angeles'),
--            SUM(offer_count), AVG(avg_price_usd)
--     FROM dws_price_offer_utc_1h
--     WHERE utc_hour >= '2025-10-01'
--     GROUP BY 1;
-- ----------------------------------------------------------------------------
CREATE TABLE dws_price_offer_utc_1h (
  utc_hour          TIMESTAMP    NOT NULL,
  partner_id        INT          NOT NULL REFERENCES dim_partner(partner_id),
  country_code      CHAR(2)      NOT NULL REFERENCES dim_country(country_code),
  product_model_id  INT          NOT NULL REFERENCES dim_product_model(product_model_id),
  payment_type      payment_type_enum NOT NULL,

  offer_count       INT          NOT NULL,
  avg_price_usd     NUMERIC(12,2),
  min_price_usd     NUMERIC(12,2),
  max_price_usd     NUMERIC(12,2),

  refreshed_at      TIMESTAMPTZ  DEFAULT NOW(),

  PRIMARY KEY (utc_hour, partner_id, country_code, product_model_id, payment_type)
) PARTITION BY RANGE (utc_hour);


-- ----------------------------------------------------------------------------
-- v_partner_price_current [VIEW — replaces former dws_cross_partner_comparison_1d]
-- ----------------------------------------------------------------------------
-- Snapshot view of CURRENT partner pricing — who's offering what right now,
-- across which partners, in which countries.
--
-- DESIGN NOTES
-- * A VIEW, not a table — zero storage, always real-time.
-- * Reads only fact_partner_price_history rows where valid_to_date IS NULL
--   (= currently active price), served by the partial index
--   idx_price_history_current. Very small result set, very fast.
-- * Historical "partner comparison on date X" is answered by querying
--   fact_partner_price_history directly with a date range predicate — no
--   need for a separate historical snapshot table.
-- * Promotion path: if this view becomes a hot query and the underlying
--   history grows, convert to MATERIALIZED VIEW with nightly REFRESH.
-- * Why a VIEW is enough here:
--     - Partner prices change ~weekly at most, so daily snapshot rows would
--       be 90% duplicates of yesterday — pre-computing is wasted storage.
--     - Current snapshot fits in memory (products × partners × countries).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_partner_price_current AS
SELECT
  h.product_model_id,
  h.country_code,
  h.payment_type,
  COUNT(DISTINCT h.partner_id)                                AS partner_count,
  MIN(h.effective_total_usd)                                  AS cheapest_price_usd,
  MAX(h.effective_total_usd) - MIN(h.effective_total_usd)     AS price_spread_usd,
  CASE WHEN AVG(h.effective_total_usd) > 0
       THEN (MAX(h.effective_total_usd) - MIN(h.effective_total_usd))
            / AVG(h.effective_total_usd)
  END                                                         AS price_spread_pct,
  jsonb_object_agg(p.partner_code, h.effective_total_usd)     AS partner_prices_json
FROM fact_partner_price_history h
JOIN dim_partner p ON p.partner_id = h.partner_id
WHERE h.valid_to_date IS NULL     -- currently active prices only
GROUP BY h.product_model_id, h.country_code, h.payment_type;


-- ----------------------------------------------------------------------------
-- dws_product_price_baseline_1d [REQUIRED]  +  mv_baseline_staging [REQUIRED]
-- ----------------------------------------------------------------------------
-- Pre-computed rolling statistics feeding the /detect-anomalies API.
-- Stored in USD so thresholds work consistently across countries.
--
-- DESIGN PATTERN: A + MV hybrid
--   * mv_baseline_staging (MATERIALIZED VIEW) encapsulates the calculation
--     logic in SQL — refreshable with one command, no separate ETL script.
--   * dws_product_price_baseline_1d (physical table) is the audit-friendly
--     persistent store. Loaded from the MV with INSERT + dedup so unchanged
--     baselines do NOT create duplicate daily rows.
--
-- WHY BOTH
--   * If we ONLY used the MV: REFRESH is full rewrite, can't dedup, no
--     historical audit trail (yesterday's baseline is lost after refresh).
--   * If we ONLY used the table: ETL calculation logic lives outside SQL,
--     harder to review / test / port.
--   * Combining: SQL-native computation + audit-ready storage + dedup.
--
-- DESIGN NOTES (on the table itself)
-- * Multiple window_days (7 / 30 / 90) for triangulation — short windows
--   catch sudden breaks, long windows catch slow drifts.
-- * p05 / p95 support quantile-based detection, robust to outliers (unlike
--   mean±std which is sensitive to skewed distributions).
-- * Refresh daily at each country's market close.
-- * PK enforces uniqueness per (baseline_as_of, product, country, window).
--   The ETL's dedup logic prevents writing a row identical to yesterday's.
-- ----------------------------------------------------------------------------
CREATE TABLE dws_product_price_baseline_1d (
  baseline_as_of    DATE         NOT NULL,
  product_model_id  INT          NOT NULL REFERENCES dim_product_model(product_model_id),
  country_code      CHAR(2)      NOT NULL REFERENCES dim_country(country_code),
  window_days       SMALLINT     NOT NULL,

  sample_size       INT          NOT NULL,
  mean_price_usd    NUMERIC(12,2),
  stddev_price_usd  NUMERIC(12,2),
  p05_price_usd     NUMERIC(12,2),
  p50_price_usd     NUMERIC(12,2),
  p95_price_usd     NUMERIC(12,2),

  refreshed_at      TIMESTAMPTZ  DEFAULT NOW(),

  PRIMARY KEY (baseline_as_of, product_model_id, country_code, window_days)
);

-- ----------------------------------------------------------------------------
-- mv_baseline_staging — computes TODAY's baseline candidates from history
-- ----------------------------------------------------------------------------
-- This MV is an intermediate computation artifact — NOT queried by APIs.
-- The daily ETL does:
--   (1) REFRESH MATERIALIZED VIEW CONCURRENTLY mv_baseline_staging;
--   (2) INSERT ... FROM mv_baseline_staging ON CONFLICT DO NOTHING
--       (with a WHERE clause skipping rows identical to yesterday's row).
--
-- ALGORITHM
-- * For each window (7 / 30 / 90 days), expand fact_partner_price_history
--   into "price active on day D" rows via a date_series CROSS JOIN.
--   This converts SCD-style change history into the daily sample needed
--   for rolling statistics.
-- * Aggregate per (product_model_id, country_code, window_days) — computing
--   mean, stddev, and three quantiles from the expanded daily samples.
-- * Filter out low-sample combinations (sample_size < 3) — they're noise.
--
-- NOTE on date_series helper: PostgreSQL's generate_series() over dates.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_baseline_staging AS
WITH windows(window_days) AS (
  VALUES (7::SMALLINT), (30::SMALLINT), (90::SMALLINT)
),
daily_prices AS (
  SELECT
    w.window_days,
    h.product_model_id,
    h.country_code,
    d::date AS obs_date,
    h.effective_total_usd AS price_usd
  FROM windows w
  CROSS JOIN generate_series(
               CURRENT_DATE - w.window_days::integer + 1,
               CURRENT_DATE,
               '1 day'::interval
             ) d
  JOIN fact_partner_price_history h
    ON d::date BETWEEN h.valid_from_date
                   AND COALESCE(h.valid_to_date, CURRENT_DATE)
)
SELECT
  CURRENT_DATE                                                 AS baseline_as_of,
  product_model_id,
  country_code,
  window_days,
  COUNT(*)                                                     AS sample_size,
  AVG(price_usd)::NUMERIC(12,2)                                AS mean_price_usd,
  STDDEV(price_usd)::NUMERIC(12,2)                             AS stddev_price_usd,
  PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY price_usd)::NUMERIC(12,2) AS p05_price_usd,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price_usd)::NUMERIC(12,2) AS p50_price_usd,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price_usd)::NUMERIC(12,2) AS p95_price_usd
FROM daily_prices
GROUP BY product_model_id, country_code, window_days
HAVING COUNT(*) >= 3;

-- Unique index required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX idx_mv_baseline_staging_pk
  ON mv_baseline_staging (baseline_as_of, product_model_id, country_code, window_days);

-- ETL pseudocode (run daily after fact_partner_price_history is updated):
/*
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_baseline_staging;

INSERT INTO dws_product_price_baseline_1d (
  baseline_as_of, product_model_id, country_code, window_days,
  sample_size, mean_price_usd, stddev_price_usd,
  p05_price_usd, p50_price_usd, p95_price_usd
)
SELECT s.*
FROM mv_baseline_staging s
WHERE NOT EXISTS (
  -- Skip write if yesterday's baseline is identical
  SELECT 1
  FROM dws_product_price_baseline_1d prev
  WHERE prev.baseline_as_of    = CURRENT_DATE - 1
    AND prev.product_model_id  = s.product_model_id
    AND prev.country_code      = s.country_code
    AND prev.window_days       = s.window_days
    AND prev.mean_price_usd    = s.mean_price_usd
    AND prev.stddev_price_usd  = s.stddev_price_usd
    AND prev.p95_price_usd     = s.p95_price_usd
)
ON CONFLICT (baseline_as_of, product_model_id, country_code, window_days)
DO NOTHING;
*/


-- ----------------------------------------------------------------------------
-- dws_partner_dq_per_batch [REQUIRED]  — event-driven DQ event log
-- ----------------------------------------------------------------------------
-- Event-driven DQ summary: exactly ONE row per load batch, INSERTed by the
-- /load-data handler at the end of each ingest. No scheduled ETL, no polling,
-- no MV refresh.
--
-- DESIGN NOTES
-- * Event-driven grain: (source_batch_id) — Partner may load 0, 1, or N times
--   per day, each load produces exactly one row. Daily rollups, if ever
--   needed, are trivially derivable via GROUP BY loaded_at::date.
-- * Why grain-per-batch (not per-day):
--     - Partners load irregularly; daily grain creates misleading empty cells
--       and hides intra-day anomalies (e.g. bad 14:00 batch merged with good
--       09:00 batch into a single "mostly OK" daily row).
--     - Each batch is an immutable event — once a load completes, its DQ
--       numbers never change. A pre-stored event record is cheaper than
--       recomputing on every query.
-- * Why a TABLE vs VIEW:
--     - Historical numbers never change → recomputation is wasted work.
--     - Survives even if source fact rows are archived out.
--     - INSERT trigger can emit real-time alerts (see commented trigger below).
-- * Populated inline with /load-data:
--       1) INSERT into fact_price_offer
--       2) run DQ rules → populate dq_output + dq_bad_records
--       3) INSERT aggregated row here
-- * anomalies_detected left NULL initially; updated by /detect-anomalies
--   when it runs on this batch.
-- ----------------------------------------------------------------------------
CREATE TABLE dws_partner_dq_per_batch (
  source_batch_id          UUID         PRIMARY KEY,
  partner_id               INT          NOT NULL REFERENCES dim_partner(partner_id),
  loaded_at                TIMESTAMPTZ  NOT NULL,

  total_records            INT          NOT NULL,
  loaded_records           INT          NOT NULL,     -- successfully written to fact_price_offer (price changed or first-time)
  rows_unchanged           INT          NOT NULL DEFAULT 0,
    -- Rows that passed DQ but had IDENTICAL price to the current
    -- fact_partner_price_history entry — intentionally NOT inserted into
    -- fact_price_offer (would be redundant). Counted here so audit can
    -- still answer "did we crawl Partner X today?" / "how stable was the
    -- price feed?" without storing duplicate event rows.
  bad_records_count        INT          NOT NULL,
  load_success_rate        NUMERIC(5,4) NOT NULL,

  harmonise_high           INT,                        -- HIGH + MANUAL confidence count
  harmonise_medium         INT,
  harmonise_low            INT,
  harmonise_high_pct       NUMERIC(5,4),

  unique_products_covered  INT,
  anomalies_detected       INT,
    -- Filled asynchronously by /detect-anomalies after it runs against this
    -- batch. Computed as: SELECT COUNT(DISTINCT offer_id) FROM fact_anomaly
    -- WHERE source_batch_id = this batch. May be UPDATEd multiple times if
    -- detection is re-run with tuned thresholds.

  created_at               TIMESTAMPTZ  DEFAULT NOW()
);

-- DESIGN: typical query = "Partner X's most recent N load batches"
CREATE INDEX idx_partner_dq_batch_time
  ON dws_partner_dq_per_batch (partner_id, loaded_at DESC);

-- ----------------------------------------------------------------------------
-- OPTIONAL: real-time low-success-rate alert via PG LISTEN/NOTIFY
-- ----------------------------------------------------------------------------
-- Uncomment to enable. Backend processes can subscribe with LISTEN dq_alert;
-- to receive immediate notifications when a batch lands with < 85% success.
-- ----------------------------------------------------------------------------
/*
CREATE OR REPLACE FUNCTION notify_low_success_rate()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.load_success_rate < 0.85 THEN
    PERFORM pg_notify(
      'dq_alert',
      json_build_object(
        'batch_id',     NEW.source_batch_id,
        'partner_id',   NEW.partner_id,
        'success_rate', NEW.load_success_rate,
        'loaded_at',    NEW.loaded_at
      )::text
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_dq_alert_on_low_rate
AFTER INSERT ON dws_partner_dq_per_batch
FOR EACH ROW EXECUTE FUNCTION notify_low_success_rate();
*/


-- ============================================================================
-- SECTION 4: DATA QUALITY TABLES
-- ============================================================================


-- ----------------------------------------------------------------------------
-- dq_output [REQUIRED]
-- ----------------------------------------------------------------------------
-- Rule-level aggregate statistics from each data-quality run.
--
-- DESIGN NOTES
-- * Grain: one row per (rule, run) — NOT per failed record. That granularity
--   belongs in dq_bad_records.
-- * Answers operational questions: "how healthy is the pipeline?", "are we
--   trending better or worse per rule over time?"
-- * Joined to dq_bad_records via dq_run_id for drill-down.
-- ----------------------------------------------------------------------------
CREATE TABLE dq_output (
  dq_run_id        BIGSERIAL    PRIMARY KEY,
  rule_id          VARCHAR(50)  NOT NULL,
  rule_name        VARCHAR(200) NOT NULL,
  rule_category    VARCHAR(50),              -- null_check / format / range / conditional
  severity         VARCHAR(10),              -- HIGH / MEDIUM / LOW
  run_ts           TIMESTAMPTZ  DEFAULT NOW(),
  total_records    INT,
  failed_records   INT,
  pass_rate        NUMERIC(5,4),
  source_batch_id  UUID                      -- load batch that triggered this run
);

CREATE INDEX idx_dq_output_rule_time ON dq_output(rule_id, run_ts DESC);


-- ----------------------------------------------------------------------------
-- dq_bad_records [REQUIRED]
-- ----------------------------------------------------------------------------
-- Row-level detail: individual records that failed DQ rules.
--
-- DESIGN NOTES
-- * raw_payload JSONB stores the ORIGINAL pre-parsed record. Even if the bad
--   data broke column types, we can still inspect the raw source — essential
--   for business-user triage (Task C-2).
-- * Workflow fields (status + assignee + resolved_at) drive a review UI:
--   business users filter status='NEW', open the raw payload,
--   apply correction (e.g. add abbreviation to dictionary), mark RESOLVED.
-- * Replay via source_batch_id: once resolved, the pipeline re-ingests only
--   the affected batch through the updated rules.
-- ----------------------------------------------------------------------------
CREATE TABLE dq_bad_records (
  bad_record_id    BIGSERIAL              PRIMARY KEY,
  source_batch_id  UUID,
  dq_run_id        BIGINT                 REFERENCES dq_output(dq_run_id),
  raw_payload      JSONB                  NOT NULL,
  rule_id          VARCHAR(50),
  failed_field     VARCHAR(100),
  error_message    TEXT,
  severity         VARCHAR(10),
  status           bad_record_status_enum DEFAULT 'NEW',
  assignee         VARCHAR(100),
  detected_at      TIMESTAMPTZ            DEFAULT NOW(),
  resolved_at      TIMESTAMPTZ,
  resolution_notes TEXT
);

CREATE INDEX idx_dq_bad_status   ON dq_bad_records(status, detected_at);
CREATE INDEX idx_dq_bad_batch    ON dq_bad_records(source_batch_id);
-- DESIGN: filter-by-assignee can be applied app-side after fetching the
-- reviewer's open queue (already narrow via idx_dq_bad_status partial scan).
-- Skip a separate assignee index.


-- ============================================================================
-- SECTION 5: USER DIMENSION — multi-stakeholder routing
-- ============================================================================
-- Strictly speaking, "user" is a dimension in the Kimball sense: future
-- fact tables (fact_user_session, fact_query_audit, etc.) would naturally
-- FK to it. The dim_ prefix keeps naming consistent with dim_role.


-- ----------------------------------------------------------------------------
-- dim_user_preference [REQUIRED for multi-stakeholder mode]
-- ----------------------------------------------------------------------------
-- DESIGN NOTES
-- * Drives API routing: Regional Sales → market_local aggregates; Finance →
--   hourly table with Pacific Time re-aggregation; Strategy → UTC aggregates.
-- * display_tz_iana: IANA name (not offset) — DST-safe.
-- * dashboard_fiscal_calendar: 'CALENDAR' vs 'APPLE_FISCAL' — Finance users
--   see FQ1/FQ2 labels; other roles see calendar Q1/Q2.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_user_preference (
  user_id                    UUID         PRIMARY KEY,
  email                      VARCHAR(255) UNIQUE NOT NULL,
  role_code                  VARCHAR(50)  NOT NULL REFERENCES dim_role(role_code),
  display_tz_iana            VARCHAR(50)  NOT NULL REFERENCES dim_timezone(iana_name),
  default_country_code       CHAR(2)      REFERENCES dim_country(country_code),
  default_currency_code      CHAR(3)      REFERENCES dim_currency(currency_code) DEFAULT 'USD',
  dashboard_fiscal_calendar  VARCHAR(20)  DEFAULT 'CALENDAR',  -- CALENDAR | APPLE_FISCAL
  created_at                 TIMESTAMPTZ  DEFAULT NOW(),
  updated_at                 TIMESTAMPTZ
);


-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
-- Post-deploy tasks (separate scripts, NOT in this file):
--   1. Backfill dim_date.fiscal_* + holiday_* using country-specific rules.
--   2. Bulk-load dim_product_model + dim_product_sku from Product Ref.csv.
--   3. Bootstrap dim_currency_rate_snapshot from ECB historical rates.
--   4. Seed dim_market_event with Apple launch dates + regional holidays.
--   5. Configure pg_partman auto-partition management for fact_* and
--      dws_* partitioned tables (see commented block under fact_price_offer).
--      Remove the DEMO hardcoded monthly partitions once pg_partman is active.
--   6. Schedule nightly ETL for aggregate refreshes.
-- ============================================================================
