-- ============================================================================
-- Task A — Database Implementation
-- ============================================================================
-- Target: PostgreSQL 14+
--
-- Requirements addressed:
--   1. Reconcile differences between partner data sources
--   2. Support multiple payment methods WITHOUT a sparse table design
--   3. Use standardised product identifiers
--   4. Track temporal data
--   5. Provide tables for dq_output and bad_records
--
-- Design patterns used:
--   * Kimball star schema           — fact + dimension separation
--   * Class Table Inheritance       — payment method polymorphism (no sparse columns)
--   * Bi-temporal facts             — business time + system time
--   * Slowly Changing Dimension Type 2 — compressed price history
--   * Range partitioning by month   — scale to billions of rows
-- ============================================================================


-- ============================================================================
-- SECTION 0: CUSTOM TYPES
-- ============================================================================

CREATE TYPE payment_type_enum
  AS ENUM ('FULL', 'INSTALMENT');

CREATE TYPE harmonise_confidence_enum
  AS ENUM ('HIGH', 'MEDIUM', 'LOW', 'MANUAL');

CREATE TYPE bad_record_status_enum
  AS ENUM ('NEW', 'IN_REVIEW', 'RESOLVED', 'IGNORED');

CREATE TYPE product_lifecycle_enum
  AS ENUM ('NEW', 'STABLE', 'LEGACY', 'EOL');


-- ============================================================================
-- SECTION 1: DIMENSION TABLES
-- ============================================================================

-- Timezone registry (IANA names, DST-safe)
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


-- Country (ISO 3166-1 alpha-2)
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


-- Currency (ISO 4217)
CREATE TABLE dim_currency (
  currency_code  CHAR(3)      PRIMARY KEY,
  currency_name  VARCHAR(100) NOT NULL,
  symbol         VARCHAR(5)
);

INSERT INTO dim_currency (currency_code, currency_name, symbol) VALUES
  ('USD', 'US Dollar',          '$'),
  ('AUD', 'Australian Dollar',  'A$'),
  ('NZD', 'New Zealand Dollar', 'NZ$'),
  ('GBP', 'British Pound',      '£'),
  ('EUR', 'Euro',               '€');


-- FX rate snapshot — periodic snapshot, one row per (pair, date)
CREATE TABLE dim_currency_rate_snapshot (
  rate_id             BIGSERIAL     PRIMARY KEY,
  from_currency_code  CHAR(3)       NOT NULL REFERENCES dim_currency(currency_code),
  to_currency_code    CHAR(3)       NOT NULL REFERENCES dim_currency(currency_code),
  rate                NUMERIC(16,8) NOT NULL CHECK (rate > 0),
  effective_date      DATE          NOT NULL,
  source              VARCHAR(50),
  ingested_at         TIMESTAMPTZ   DEFAULT NOW(),

  UNIQUE (from_currency_code, to_currency_code, effective_date)
);

CREATE INDEX idx_dim_fx_lookup
  ON dim_currency_rate_snapshot (from_currency_code, effective_date DESC);


-- Calendar dimension — pre-generated dates with business attributes
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
  day_of_week            SMALLINT,
  day_name               VARCHAR(20),
  is_weekend             BOOLEAN,

  -- Apple fiscal calendar (fiscal year ends late September; FQ1 = Oct-Dec)
  fiscal_year            SMALLINT,
  fiscal_quarter         SMALLINT,
  fiscal_period          SMALLINT,
  is_fiscal_quarter_end  BOOLEAN,

  -- Per-country holiday flags
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


-- Partner registry
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


-- Product category (carries category-level business attributes)
CREATE TABLE dim_product_category (
  category_id                 SERIAL       PRIMARY KEY,
  category_code               VARCHAR(20)  UNIQUE NOT NULL,
  category_name               VARCHAR(100) NOT NULL,
  display_order               INT          DEFAULT 0,

  default_warranty_months     SMALLINT,
  default_return_window_days  SMALLINT,
  has_carrier_variants        BOOLEAN      DEFAULT FALSE,

  lifecycle_months            SMALLINT,
  price_volatility_class      VARCHAR(20),

  icon_url                    TEXT
);

INSERT INTO dim_product_category (
  category_code, category_name, display_order,
  default_warranty_months, default_return_window_days, has_carrier_variants,
  lifecycle_months, price_volatility_class, icon_url
) VALUES
  ('IPHONE',  'iPhone',      1, 12, 14, TRUE,  12, 'MEDIUM', '/icons/iphone.svg'),
  ('IPAD',    'iPad',        2, 12, 14, TRUE,  18, 'LOW',    '/icons/ipad.svg'),
  ('MAC',     'Mac',         3, 12, 14, FALSE, 24, 'LOW',    '/icons/mac.svg'),
  ('AIRPODS', 'AirPods',     4, 12, 14, FALSE, 24, 'HIGH',   '/icons/airpods.svg'),
  ('WATCH',   'Apple Watch', 5, 12, 14, TRUE,  12, 'MEDIUM', '/icons/watch.svg');


-- Standardised product model (target of harmonise step)
-- Double key: surrogate (id) for joins + natural (model_key) for idempotency
CREATE TABLE dim_product_model (
  product_model_id  SERIAL       PRIMARY KEY,
  model_key         VARCHAR(200) UNIQUE NOT NULL,    -- e.g. 'iphone_17_pro_max_512gb'
  category_id       INT          NOT NULL REFERENCES dim_product_category(category_id),
  model_line        VARCHAR(100),
  chip              VARCHAR(50),
  storage_gb        INT,
  connectivity      VARCHAR(20),

  launch_date       DATE,
  eol_date          DATE,
  lifecycle_status  product_lifecycle_enum DEFAULT 'STABLE',

  created_at        TIMESTAMPTZ  DEFAULT NOW(),
  updated_at        TIMESTAMPTZ  DEFAULT NOW()
);


-- SKU level (color variants) — kept for traceability back to source Ref CSV
CREATE TABLE dim_product_sku (
  sku_id            SERIAL       PRIMARY KEY,
  product_model_id  INT          NOT NULL REFERENCES dim_product_model(product_model_id),
  ref_product_id    INT,
  color             VARCHAR(50),
  short_desc        TEXT,
  long_desc         TEXT
);


-- Known market events (Apple launches, holidays). Anomaly detector reads this
-- to suppress false positives within a configurable window around each event.
CREATE TABLE dim_market_event (
  event_id                 SERIAL       PRIMARY KEY,
  event_date               DATE         NOT NULL,
  event_type               VARCHAR(50)  NOT NULL,
  event_name               TEXT,
  affected_country_code    CHAR(2),
  affected_category_code   VARCHAR(20),
  suppression_window_days  SMALLINT     DEFAULT 7
);


-- Stakeholder role taxonomy. Drives API routing in dim_user_preference.
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


-- Tunable parameters for anomaly detection.
-- Most-specific scope wins (category + country > category > country > global).
CREATE TABLE dim_anomaly_threshold (
  threshold_id      SERIAL        PRIMARY KEY,
  config_key        VARCHAR(80)   NOT NULL,
  config_value      NUMERIC(12,4) NOT NULL,
  config_type       VARCHAR(30),         -- 'weight' / 'threshold' / 'factor' / 'percentile'

  category_code     VARCHAR(20)   REFERENCES dim_product_category(category_code),
  country_code      CHAR(2)       REFERENCES dim_country(country_code),

  rationale         TEXT          NOT NULL,
  source            VARCHAR(30)   NOT NULL,   -- 'initial_guess' / 'data_calibrated' / 'business_agreed'
  last_reviewed_at  DATE          DEFAULT CURRENT_DATE,
  updated_by        VARCHAR(100),

  UNIQUE (config_key, category_code, country_code)
);

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
    (category_code IS NOT NULL)::int DESC,
    (country_code  IS NOT NULL)::int DESC
  LIMIT 1;
$$ LANGUAGE SQL STABLE;

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

  ('lifecycle_factor_NEW',         0.30, 'factor',    'NEW products have unstable baselines, reduce sensitivity', 'initial_guess'),
  ('lifecycle_factor_STABLE',      1.00, 'factor',    'Default — no adjustment', 'initial_guess'),
  ('lifecycle_factor_LEGACY',      0.60, 'factor',    'Legacy products naturally drop in price', 'initial_guess'),
  ('lifecycle_factor_EOL',         0.00, 'factor',    'EOL products suppressed entirely — no anomaly alerts', 'business_agreed'),

  ('event_suppression_APPLE_LAUNCH', 0.40, 'factor',  'A/B test against labeled launch events', 'initial_guess'),
  ('event_suppression_BLACK_FRIDAY', 0.50, 'factor',  'Initial guess', 'initial_guess'),
  ('event_suppression_BOXING_DAY',   0.50, 'factor',  'Initial guess', 'initial_guess'),

  ('category_sensitivity_LOW',     1.00, 'factor',    'LOW volatility category (e.g., Mac)', 'initial_guess'),
  ('category_sensitivity_MEDIUM',  1.30, 'factor',    'MEDIUM volatility (e.g., iPhone)', 'initial_guess'),
  ('category_sensitivity_HIGH',    1.80, 'factor',    'HIGH volatility (e.g., AirPods — promo-heavy)', 'initial_guess'),

  ('sku_variance_threshold_pct',   0.03, 'threshold', 'Max intra-partner intra-product price spread before SKU_VARIANCE anomaly fires', 'initial_guess'),

  ('min_baseline_sample_size',     14,   'threshold', 'Baselines below this are unreliable — fallback to cross-partner baseline', 'initial_guess');


-- Notification channel registry
CREATE TABLE dim_alert_channel (
  channel_id     SERIAL        PRIMARY KEY,
  channel_code   VARCHAR(30)   UNIQUE NOT NULL,
  channel_name   VARCHAR(100)  NOT NULL,
  config_json    JSONB,
  is_active      BOOLEAN       DEFAULT TRUE,
  created_at     TIMESTAMPTZ   DEFAULT NOW()
);

INSERT INTO dim_alert_channel (channel_code, channel_name, is_active) VALUES
  ('SLACK',   'Slack workspace',       TRUE),
  ('TEAMS',   'Microsoft Teams',       TRUE),
  ('EMAIL',   'Email (daily digest)',  TRUE),
  ('WEBHOOK', 'Generic webhook (Jira/Linear ticket creation)', TRUE);


-- Routing rules: which anomalies fire to which channel/recipients
CREATE TABLE dim_alert_policy (
  policy_id          SERIAL       PRIMARY KEY,
  policy_name        VARCHAR(100) NOT NULL,
  min_severity       VARCHAR(10)  NOT NULL,
  anomaly_type       VARCHAR(30),
  country_code       CHAR(2)      REFERENCES dim_country(country_code),
  category_code      VARCHAR(20)  REFERENCES dim_product_category(category_code),
  channel_id         INT          REFERENCES dim_alert_channel(channel_id),
  recipient          VARCHAR(200),
  recipient_type     VARCHAR(20),
  quiet_hours_start  TIME,
  quiet_hours_end    TIME,
  digest_send_time   TIME,
  is_active          BOOLEAN      DEFAULT TRUE,
  created_at         TIMESTAMPTZ  DEFAULT NOW()
);


-- ============================================================================
-- SECTION 2: FACT TABLES — EVENT GRAIN
-- ============================================================================

-- Parent fact table (Class Table Inheritance pattern).
-- Stores common attributes; payment-specific fields live in child tables.
-- Bi-temporal: crawl_ts_utc (business) + ingested_at (system).
-- Timezone-aware: stores both UTC and country-local time.
-- Currency-frozen: local + USD + FX rate captured at load time for audit.
CREATE TABLE fact_price_offer (
  offer_id                 BIGSERIAL   NOT NULL,

  partner_id               INT         NOT NULL REFERENCES dim_partner(partner_id),
  country_code             CHAR(2)     NOT NULL REFERENCES dim_country(country_code),
  product_model_id         INT         REFERENCES dim_product_model(product_model_id),
  sku_id                   INT         REFERENCES dim_product_sku(sku_id),

  raw_product_name         TEXT        NOT NULL,
  payment_type             payment_type_enum NOT NULL,    -- CTI discriminator

  -- Money: original + normalized + FX audit trail
  currency_code            CHAR(3)     NOT NULL REFERENCES dim_currency(currency_code),
  effective_total_local    NUMERIC(12,2) NOT NULL CHECK (effective_total_local > 0),
  effective_total_usd      NUMERIC(12,2) NOT NULL CHECK (effective_total_usd > 0),
  fx_rate_to_usd           NUMERIC(16,8) NOT NULL,
  fx_rate_date             DATE        NOT NULL,
  fx_rate_source           VARCHAR(50),

  -- Temporal (bi-temporal + timezone-aware)
  crawl_ts_utc             TIMESTAMPTZ NOT NULL,
  crawl_ts_local           TIMESTAMP   NOT NULL,
  crawl_date_local         DATE        GENERATED ALWAYS AS (crawl_ts_local::date) STORED,
  ingested_at              TIMESTAMPTZ DEFAULT NOW(),

  -- Harmonise provenance
  harmonise_score          NUMERIC(4,3),
  harmonise_confidence     harmonise_confidence_enum,

  -- Batch tracking (idempotency + replay)
  source_batch_id          UUID,

  PRIMARY KEY (offer_id, crawl_ts_utc)
) PARTITION BY RANGE (crawl_ts_utc);

-- Single DEFAULT partition. In production, pg_partman would auto-create
-- monthly partitions and detach rows from default into them on a schedule.
CREATE TABLE fact_price_offer_default PARTITION OF fact_price_offer DEFAULT;

CREATE INDEX idx_offer_model_ts          ON fact_price_offer(product_model_id, crawl_ts_utc);
CREATE INDEX idx_offer_partner_ts        ON fact_price_offer(partner_id, crawl_ts_utc);
CREATE INDEX idx_offer_country_date_local ON fact_price_offer(country_code, crawl_date_local);
CREATE INDEX idx_offer_low_conf
  ON fact_price_offer(harmonise_confidence, ingested_at)
  WHERE harmonise_confidence IN ('LOW', 'MEDIUM');


-- Child table: FULL payment variant
CREATE TABLE fact_payment_full_price (
  offer_id      BIGINT        NOT NULL,
  crawl_ts_utc  TIMESTAMPTZ   NOT NULL,
  full_price    NUMERIC(12,2) NOT NULL CHECK (full_price > 0),

  PRIMARY KEY (offer_id, crawl_ts_utc),
  FOREIGN KEY (offer_id, crawl_ts_utc)
    REFERENCES fact_price_offer(offer_id, crawl_ts_utc)
) PARTITION BY RANGE (crawl_ts_utc);

CREATE TABLE fact_payment_full_price_default PARTITION OF fact_payment_full_price DEFAULT;


-- Child table: INSTALMENT payment variant
CREATE TABLE fact_payment_instalment (
  offer_id           BIGINT        NOT NULL,
  crawl_ts_utc       TIMESTAMPTZ   NOT NULL,
  monthly_amount     NUMERIC(12,2) NOT NULL CHECK (monthly_amount > 0),
  instalment_months  SMALLINT      NOT NULL CHECK (instalment_months BETWEEN 1 AND 60),

  PRIMARY KEY (offer_id, crawl_ts_utc),
  FOREIGN KEY (offer_id, crawl_ts_utc)
    REFERENCES fact_price_offer(offer_id, crawl_ts_utc)
) PARTITION BY RANGE (crawl_ts_utc);

CREATE TABLE fact_payment_instalment_default PARTITION OF fact_payment_instalment DEFAULT;


-- Slowly Changing Dimension Type 2 — compressed price history.
-- Only inserts a new row when the price actually changes.
-- Source: derived from fact_price_offer by incremental ETL after each batch.
CREATE TABLE fact_partner_price_history (
  history_id             BIGSERIAL    PRIMARY KEY,

  product_model_id       INT          NOT NULL REFERENCES dim_product_model(product_model_id),
  partner_id             INT          NOT NULL REFERENCES dim_partner(partner_id),
  country_code           CHAR(2)      NOT NULL REFERENCES dim_country(country_code),
  payment_type           payment_type_enum NOT NULL,
  sku_id                 INT          REFERENCES dim_product_sku(sku_id),

  currency_code          CHAR(3)      NOT NULL REFERENCES dim_currency(currency_code),
  effective_total_local  NUMERIC(12,2) NOT NULL CHECK (effective_total_local > 0),
  effective_total_usd    NUMERIC(12,2) NOT NULL CHECK (effective_total_usd > 0),

  valid_from_date        DATE         NOT NULL,
  valid_to_date          DATE,                              -- NULL = currently active

  created_at             TIMESTAMPTZ  DEFAULT NOW(),

  UNIQUE (product_model_id, partner_id, country_code, payment_type, sku_id, valid_from_date),
  CHECK (valid_to_date IS NULL OR valid_to_date >= valid_from_date)
);

CREATE INDEX idx_price_history_current
  ON fact_partner_price_history (product_model_id, country_code)
  WHERE valid_to_date IS NULL;

CREATE INDEX idx_price_history_date_range
  ON fact_partner_price_history (product_model_id, country_code,
                                 valid_from_date, valid_to_date);


-- Detected anomalies — one row per triggered signal (not per offer).
-- An offer that trips multiple signals appears as multiple anomaly rows,
-- each routed independently per dim_alert_policy.
CREATE TABLE fact_anomaly (
  anomaly_id                BIGSERIAL     PRIMARY KEY,

  offer_id                  BIGINT        NOT NULL,
  crawl_ts_utc              TIMESTAMPTZ   NOT NULL,
  source_batch_id           UUID,

  product_model_id          INT           REFERENCES dim_product_model(product_model_id),
  partner_id                INT           REFERENCES dim_partner(partner_id),
  country_code              CHAR(2)       REFERENCES dim_country(country_code),

  anomaly_type              VARCHAR(30)   NOT NULL,    -- STATISTICAL / TEMPORAL / CROSS_PARTNER / SKU_VARIANCE
  signal_score              NUMERIC(4,3)  NOT NULL,
  severity                  VARCHAR(10)   NOT NULL,    -- HIGH / MEDIUM / LOW

  lifecycle_factor          NUMERIC(4,2),
  event_suppression_factor  NUMERIC(4,2),
  category_sensitivity      NUMERIC(4,2),

  observed_price_usd        NUMERIC(12,2),
  baseline_snapshot         JSONB,
  threshold_snapshot        JSONB,        -- freezes dim_anomaly_threshold values for replay-safety

  suppression_applied       BOOLEAN       DEFAULT FALSE,
  suppression_event_id      INT           REFERENCES dim_market_event(event_id),

  status                    VARCHAR(20)   DEFAULT 'NEW',
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


-- ============================================================================
-- SECTION 3: SUMMARY TABLES (DWS LAYER)
-- ============================================================================

-- Daily UTC aggregation
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


-- Hourly UTC aggregation — enables on-the-fly rollup into any timezone
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


-- Snapshot view of CURRENT partner pricing across partners + countries.
-- Reads only fact_partner_price_history rows where valid_to_date IS NULL.
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
WHERE h.valid_to_date IS NULL
GROUP BY h.product_model_id, h.country_code, h.payment_type;


-- Pre-computed rolling baselines feeding /detect-anomalies.
-- Stored in USD so thresholds work consistently across countries.
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


-- Materialized view feeding the baseline table above.
-- Daily ETL: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_baseline_staging;
--            INSERT INTO dws_product_price_baseline_1d ... ON CONFLICT DO NOTHING.
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
  CURRENT_DATE                                                          AS baseline_as_of,
  product_model_id,
  country_code,
  window_days,
  COUNT(*)                                                              AS sample_size,
  AVG(price_usd)::NUMERIC(12,2)                                         AS mean_price_usd,
  STDDEV(price_usd)::NUMERIC(12,2)                                      AS stddev_price_usd,
  PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY price_usd)::NUMERIC(12,2) AS p05_price_usd,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price_usd)::NUMERIC(12,2) AS p50_price_usd,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price_usd)::NUMERIC(12,2) AS p95_price_usd
FROM daily_prices
GROUP BY product_model_id, country_code, window_days
HAVING COUNT(*) >= 3;

CREATE UNIQUE INDEX idx_mv_baseline_staging_pk
  ON mv_baseline_staging (baseline_as_of, product_model_id, country_code, window_days);


-- Event-driven DQ summary — exactly ONE row per load batch.
-- Populated inline by /load-data after the pipeline completes.
CREATE TABLE dws_partner_dq_per_batch (
  source_batch_id          UUID         PRIMARY KEY,
  partner_id               INT          NOT NULL REFERENCES dim_partner(partner_id),
  loaded_at                TIMESTAMPTZ  NOT NULL,

  total_records            INT          NOT NULL,
  loaded_records           INT          NOT NULL,
  rows_unchanged           INT          NOT NULL DEFAULT 0,
  bad_records_count        INT          NOT NULL,
  load_success_rate        NUMERIC(5,4) NOT NULL,

  harmonise_high           INT,
  harmonise_medium         INT,
  harmonise_low            INT,
  harmonise_high_pct       NUMERIC(5,4),

  unique_products_covered  INT,
  anomalies_detected       INT,

  created_at               TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_partner_dq_batch_time
  ON dws_partner_dq_per_batch (partner_id, loaded_at DESC);


-- ============================================================================
-- SECTION 4: DATA QUALITY TABLES (assignment requirement)
-- ============================================================================

-- Rule-level summary: one row per (rule, run). Powers DQ trend dashboards.
CREATE TABLE dq_output (
  dq_run_id        BIGSERIAL    PRIMARY KEY,
  rule_id          VARCHAR(50)  NOT NULL,
  rule_name        VARCHAR(200) NOT NULL,
  rule_category    VARCHAR(50),
  severity         VARCHAR(10),
  run_ts           TIMESTAMPTZ  DEFAULT NOW(),
  total_records    INT,
  failed_records   INT,
  pass_rate        NUMERIC(5,4),
  source_batch_id  UUID
);

CREATE INDEX idx_dq_output_rule_time ON dq_output(rule_id, run_ts DESC);


-- Row-level detail: one row per failing record.
-- raw_payload (JSONB) preserves the original record even if its types broke.
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

CREATE INDEX idx_dq_bad_status ON dq_bad_records(status, detected_at);
CREATE INDEX idx_dq_bad_batch  ON dq_bad_records(source_batch_id);


-- ============================================================================
-- SECTION 5: USER DIMENSION — multi-stakeholder routing
-- ============================================================================

-- Drives API routing per role + display preferences (timezone, fiscal calendar)
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
