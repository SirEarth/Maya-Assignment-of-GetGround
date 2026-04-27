-- ============================================================================
-- rules.sql — Data Quality rule catalog
-- ============================================================================
-- Companion to schema.sql. Provides:
--   1. stg_price_offer           — ingestion staging table
--   2. dq_rule_catalog           — master registry of active rules
--   3. dq_check_* functions      — one per rule, each returns violations
--   4. dq_run_batch()            — orchestrator: runs all active rules,
--                                  writes dq_output + dq_bad_records
--
-- Rules are split into three stages:
--   INGEST-stage   run on raw stg_price_offer (parse / format failures).
--                  Failing rows stop here.
--   PRE_FACT-stage run on enriched stg AFTER harmonise (HIGH-severity gate:
--                  country↔currency, partner↔country, harmonise unmatched).
--                  Failing rows STOP HERE — they never enter fact_price_offer.
--   SEMANTIC-stage run on fact_price_offer AFTER load (soft signals only:
--                  low-confidence harmonise + category sanity bounds).
--                  Failing rows STAY in fact, just flagged for triage.
-- ============================================================================


-- ============================================================================
-- SECTION A: STAGING TABLE (pre-INSERT landing zone)
-- ============================================================================
-- /load-data writes each parsed CSV row here first. DQ runs, valid rows move
-- to fact_price_offer, invalid rows stay with raw_payload for business review.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_price_offer (
  stg_row_id          BIGSERIAL    PRIMARY KEY,
  source_batch_id     UUID         NOT NULL,
  row_num             INT          NOT NULL,             -- original CSV line number
  raw_payload         JSONB        NOT NULL,             -- the full raw row for audit

  -- Parsed fields (ALL nullable — the DQ engine decides what's valid)
  partner_code        VARCHAR(50),
  country_name        VARCHAR(100),                       -- 'Australia' / 'New Zealand'
  country_code        CHAR(2),                            -- looked up from dim_country
  raw_product_name    TEXT,
  crawl_ts_raw        TEXT,                               -- original string
  crawl_ts_utc        TIMESTAMPTZ,                        -- parsed
  currency_code       CHAR(3),                            -- looked up from country
  payment_type        VARCHAR(20),
  full_price          NUMERIC(12,2),
  monthly_amount      NUMERIC(12,2),
  instalment_months   SMALLINT,

  -- Harmonise output (populated before DQ runs)
  product_model_id      INT,
  harmonise_score       NUMERIC(4,3),
  harmonise_confidence  VARCHAR(10),

  -- DQ status
  dq_status           VARCHAR(20) DEFAULT 'PENDING',     -- PENDING / PASSED / FAILED
  validated_at        TIMESTAMPTZ,

  UNIQUE (source_batch_id, row_num)
);

CREATE INDEX IF NOT EXISTS idx_stg_batch   ON stg_price_offer(source_batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_status  ON stg_price_offer(dq_status);


-- ============================================================================
-- SECTION B: RULE CATALOG
-- ============================================================================
-- Registry of all DQ rules. Rules are activated/deactivated here rather than
-- in code — so business can disable a noisy rule without a deploy.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_rule_catalog (
  rule_id             VARCHAR(50)  PRIMARY KEY,
  rule_name           VARCHAR(200) NOT NULL,
  rule_category       VARCHAR(50)  NOT NULL,             -- null_check/format/range/conditional/duplicate/harmonise/consistency/referential
  severity            VARCHAR(10)  NOT NULL,             -- HIGH/MEDIUM/LOW
  description         TEXT,
  target_stage        VARCHAR(20)  NOT NULL,             -- INGEST (stg parse) / PRE_FACT (stg gate before fact insert) / SEMANTIC (fact post-load)
  check_function_name VARCHAR(100) NOT NULL,
  is_active           BOOLEAN      DEFAULT TRUE,
  created_at          TIMESTAMPTZ  DEFAULT NOW(),
  last_reviewed_at    DATE         DEFAULT CURRENT_DATE
);

INSERT INTO dq_rule_catalog (rule_id, rule_name, rule_category, severity, description, target_stage, check_function_name) VALUES
  -- --- INGEST stage: parse + format + required -------------------------------
  ('DQ_NULL_001',   'Required: crawl_ts',             'null_check', 'HIGH',   'Timestamp must be present and parseable as ISO 8601',      'INGEST',   'dq_check_null_crawl_ts'),
  ('DQ_NULL_002',   'Required: country',              'null_check', 'HIGH',   'Country must be present and resolvable to an ISO code',    'INGEST',   'dq_check_null_country'),
  ('DQ_NULL_004',   'Required: product name',         'null_check', 'HIGH',   'Raw product name must not be empty',                        'INGEST',   'dq_check_null_product_name'),
  ('DQ_FMT_001',    'Format: country code resolves',  'format',     'HIGH',   'country_name must map to a known ISO code in dim_country', 'INGEST',   'dq_check_country_resolves'),
  ('DQ_RANGE_001',  'Range: full_price positive',     'range',      'HIGH',   'full_price > 0 for FULL payment type',                      'INGEST',   'dq_check_full_price_positive'),
  ('DQ_RANGE_002',  'Range: monthly_amount positive', 'range',      'HIGH',   'monthly_amount > 0 for INSTALMENT payment type',            'INGEST',   'dq_check_monthly_amount_positive'),
  ('DQ_RANGE_003',  'Range: instalment_months 1-60',  'range',      'HIGH',   'instalment_months must be 1-60 for INSTALMENT',             'INGEST',   'dq_check_instalment_months_range'),
  ('DQ_COND_001',   'Conditional: payment fields',    'conditional','HIGH',   'FULL needs full_price; INSTALMENT needs monthly+months',    'INGEST',   'dq_check_payment_fields_conditional'),

  -- --- PRE_FACT stage: HIGH-severity gate; failing rows DO NOT enter fact -----
  -- These rules check enriched stg (after harmonise) and block factual errors
  -- from polluting fact_price_offer. Run after Step 3 (harmonise), before Step 4
  -- (build facts). See services.py submit_load_job pipeline.
  ('DQ_CONS_001',   'Consistency: country↔currency',  'consistency','HIGH',   'AU rows should have AUD; NZ rows should have NZD (etc)',    'PRE_FACT', 'dq_check_country_currency_match_stg'),
  ('DQ_CONS_002',   'Consistency: partner country',   'consistency','HIGH',   'Partner country_code should match the row country',         'PRE_FACT', 'dq_check_partner_country_match_stg'),
  ('DQ_HARM_002',   'Harmonise: unmatched',           'harmonise',  'HIGH',   'product_model_id IS NULL (no match at all)',                'PRE_FACT', 'dq_check_harmonise_unmatched_stg'),

  -- --- SEMANTIC stage: soft signals; failing rows STAY in fact, only flagged --
  -- Run on fact_price_offer after Step 4. Business reviews via /bad-records.
  -- Scope is intentionally narrow: only single-row data-quality concerns that
  -- require business judgment. Pricing-pattern anomalies (cross-row variance,
  -- temporal jumps, cross-partner divergence) live in /detect-anomalies, not here.
  ('DQ_HARM_001',   'Harmonise: low confidence',      'harmonise',  'MEDIUM', 'harmonise_confidence = LOW needs business review',          'SEMANTIC', 'dq_check_harmonise_low_confidence'),
  ('DQ_PRICE_001',  'Price: category sanity',         'range',      'MEDIUM', 'effective_total_usd outside reasonable range for category', 'SEMANTIC', 'dq_check_price_category_sanity')
ON CONFLICT (rule_id) DO NOTHING;

-- Migration: idempotent removal of rules retired from earlier iterations.
-- DQ_DUP_001 / DQ_DUP_002 — moved to ops health monitoring (scraper bugs,
--   not data-quality issues). DQ_FX_001 — false-positive heavy on weekends;
--   USD precision drift handled at query-time recompute instead.
-- DQ_SKU_001 — duplicates the SKU_VARIANCE anomaly signal; cross-row pricing
--   patterns belong in /detect-anomalies, not in single-row DQ.
-- DQ_NULL_003 / DQ_FMT_002 — defensive checks for inputs already validated
--   upstream (partner_code is API-validated; currency_code is derived from
--   country_code via inline dict over a closed set). Could not fire through
--   the live API path; removed to keep the rule catalog signal-rich.
DELETE FROM dq_rule_catalog WHERE rule_id IN
  ('DQ_DUP_001','DQ_DUP_002','DQ_FX_001','DQ_SKU_001','DQ_NULL_003','DQ_FMT_002');
DROP FUNCTION IF EXISTS dq_check_duplicates_exact(UUID);
DROP FUNCTION IF EXISTS dq_check_duplicates_near(UUID);
DROP FUNCTION IF EXISTS dq_check_fx_rate_stale(UUID);
DROP FUNCTION IF EXISTS dq_check_sku_variance(UUID);
DROP FUNCTION IF EXISTS dq_check_null_partner(UUID);
DROP FUNCTION IF EXISTS dq_check_currency_resolves(UUID);

-- For idempotent re-runs on existing databases: force-update the 3 rules that
-- moved from SEMANTIC to PRE_FACT. ON CONFLICT DO NOTHING above is a no-op for
-- existing rows; this UPDATE makes the migration explicit.
UPDATE dq_rule_catalog SET target_stage='PRE_FACT', severity='HIGH',
  check_function_name='dq_check_country_currency_match_stg'
  WHERE rule_id='DQ_CONS_001';
UPDATE dq_rule_catalog SET target_stage='PRE_FACT', severity='HIGH',
  check_function_name='dq_check_partner_country_match_stg'
  WHERE rule_id='DQ_CONS_002';
UPDATE dq_rule_catalog SET target_stage='PRE_FACT', severity='HIGH',
  check_function_name='dq_check_harmonise_unmatched_stg'
  WHERE rule_id='DQ_HARM_002';


-- ============================================================================
-- SECTION C: RULE CHECK FUNCTIONS
-- ============================================================================
-- Consistent signature across all functions:
--   INPUT:  p_batch_id UUID
--   OUTPUT: TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB)
--
-- The engine wraps each call, counts rows, and writes to dq_output + dq_bad_records.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- INGEST-stage rules (run against stg_price_offer)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION dq_check_null_crawl_ts(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT stg_row_id, 'crawl_ts_utc'::TEXT,
         'crawl_ts is NULL or failed to parse as ISO 8601'::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND crawl_ts_utc IS NULL;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_null_country(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT stg_row_id, 'country_name'::TEXT,
         'country is NULL or empty'::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND (country_name IS NULL OR TRIM(country_name) = '');
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_null_product_name(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT stg_row_id, 'raw_product_name'::TEXT,
         'product name is NULL or empty'::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND (raw_product_name IS NULL OR TRIM(raw_product_name) = '');
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_country_resolves(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT stg_row_id, 'country_code'::TEXT,
         ('country_name "' || country_name || '" did not resolve to an ISO code')::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND country_name IS NOT NULL
    AND country_code IS NULL;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_full_price_positive(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT stg_row_id, 'full_price'::TEXT,
         ('full_price must be > 0, got ' || full_price)::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND payment_type = 'FULL'
    AND (full_price IS NULL OR full_price <= 0);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_monthly_amount_positive(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT stg_row_id, 'monthly_amount'::TEXT,
         ('monthly_amount must be > 0 for INSTALMENT, got ' || COALESCE(monthly_amount::TEXT, 'NULL'))::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND payment_type = 'INSTALMENT'
    AND (monthly_amount IS NULL OR monthly_amount <= 0);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_instalment_months_range(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT stg_row_id, 'instalment_months'::TEXT,
         ('instalment_months must be 1..60, got ' || COALESCE(instalment_months::TEXT, 'NULL'))::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND payment_type = 'INSTALMENT'
    AND (instalment_months IS NULL OR instalment_months < 1 OR instalment_months > 60);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_payment_fields_conditional(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  -- FULL must have full_price AND no instalment fields
  SELECT stg_row_id, 'payment_type'::TEXT,
         'FULL payment requires full_price but has instalment fields set'::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND payment_type = 'FULL'
    AND (monthly_amount IS NOT NULL OR instalment_months IS NOT NULL)

  UNION ALL

  -- INSTALMENT must have monthly+months AND no full_price
  SELECT stg_row_id, 'payment_type'::TEXT,
         'INSTALMENT payment requires monthly + months but has full_price set'::TEXT,
         raw_payload
  FROM stg_price_offer
  WHERE source_batch_id = p_batch_id
    AND payment_type = 'INSTALMENT'
    AND full_price IS NOT NULL;
$$ LANGUAGE SQL STABLE;


-- ----------------------------------------------------------------------------
-- PRE_FACT-stage rules (run against enriched stg_price_offer; HIGH-severity
-- gate — rows failing here NEVER enter fact_price_offer)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION dq_check_country_currency_match_stg(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  -- Expected currency per country — inline table; could be promoted to dim_country
  WITH expected AS (
    SELECT * FROM (VALUES
      ('AU', 'AUD'), ('NZ', 'NZD'), ('US', 'USD'), ('GB', 'GBP')
    ) AS t(country_code, expected_currency)
  )
  SELECT s.stg_row_id,
         'currency_code'::TEXT,
         ('country=' || s.country_code || ' expected ' || e.expected_currency ||
          ' but got ' || COALESCE(s.currency_code, 'NULL'))::TEXT,
         s.raw_payload
  FROM stg_price_offer s
  JOIN expected e ON e.country_code = s.country_code
  WHERE s.source_batch_id = p_batch_id
    AND s.dq_status       = 'INGEST_PASSED'
    AND s.currency_code IS NOT NULL
    AND s.currency_code != e.expected_currency;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_partner_country_match_stg(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT s.stg_row_id,
         'country_code'::TEXT,
         ('partner ' || p.partner_code || ' registered under ' || p.country_code ||
          ' but row claims ' || s.country_code)::TEXT,
         s.raw_payload
  FROM stg_price_offer s
  JOIN dim_partner p ON p.partner_code = s.partner_code
  WHERE s.source_batch_id = p_batch_id
    AND s.dq_status       = 'INGEST_PASSED'
    AND p.country_code  IS NOT NULL
    AND s.country_code  IS NOT NULL
    AND p.country_code  != s.country_code;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_harmonise_unmatched_stg(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT s.stg_row_id,
         'product_model_id'::TEXT,
         ('no harmonise match: raw="' || s.raw_product_name || '"')::TEXT,
         s.raw_payload
  FROM stg_price_offer s
  WHERE s.source_batch_id = p_batch_id
    AND s.dq_status       = 'INGEST_PASSED'
    AND s.product_model_id IS NULL;
$$ LANGUAGE SQL STABLE;


-- ----------------------------------------------------------------------------
-- SEMANTIC-stage rules (run against fact_price_offer after load — soft signals
-- only; failing rows STAY in fact, just flagged in dq_bad_records)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION dq_check_harmonise_low_confidence(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  SELECT f.offer_id,
         'harmonise_confidence'::TEXT,
         ('low harmonise confidence (score=' || COALESCE(f.harmonise_score::TEXT, 'NULL') ||
          '): raw="' || f.raw_product_name || '"')::TEXT,
         jsonb_build_object(
           'offer_id', f.offer_id,
           'raw_product_name', f.raw_product_name,
           'product_model_id', f.product_model_id,
           'harmonise_score', f.harmonise_score
         )
  FROM fact_price_offer f
  WHERE f.source_batch_id = p_batch_id
    AND f.harmonise_confidence = 'LOW';
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dq_check_price_category_sanity(p_batch_id UUID)
RETURNS TABLE (row_ref BIGINT, failed_field TEXT, error_message TEXT, raw_payload JSONB) AS $$
  -- Category-level sanity bands. Inline for now; could live in
  -- dim_product_category as new columns (min_expected_usd, max_expected_usd).
  WITH bands AS (
    SELECT * FROM (VALUES
      ('IPHONE',   400::NUMERIC,  3500::NUMERIC),
      ('IPAD',     300::NUMERIC,  4000::NUMERIC),
      ('MAC',      800::NUMERIC,  8000::NUMERIC),
      ('AIRPODS',   80::NUMERIC,   800::NUMERIC),
      ('WATCH',    200::NUMERIC,  2500::NUMERIC)
    ) AS t(category_code, min_usd, max_usd)
  )
  SELECT f.offer_id,
         'effective_total_usd'::TEXT,
         ('category=' || c.category_code ||
          ': price ' || f.effective_total_usd ||
          ' outside expected [' || b.min_usd || ', ' || b.max_usd || ']')::TEXT,
         jsonb_build_object(
           'offer_id', f.offer_id,
           'category', c.category_code,
           'price_usd', f.effective_total_usd,
           'min_usd', b.min_usd, 'max_usd', b.max_usd
         )
  FROM fact_price_offer f
  JOIN dim_product_model m ON m.product_model_id = f.product_model_id
  JOIN dim_product_category c ON c.category_id = m.category_id
  JOIN bands b ON b.category_code = c.category_code
  WHERE f.source_batch_id = p_batch_id
    AND (f.effective_total_usd < b.min_usd OR f.effective_total_usd > b.max_usd);
$$ LANGUAGE SQL STABLE;


-- ============================================================================
-- SECTION D: ORCHESTRATOR
-- ============================================================================
-- Runs all active rules for a given batch, populates dq_output + dq_bad_records.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dq_run_batch(p_batch_id UUID)
RETURNS TABLE (rule_id VARCHAR, total_records INT, failed_records INT, pass_rate NUMERIC) AS $$
DECLARE
  r         RECORD;
  total_cnt INT;
  failed_cnt INT;
  run_id    BIGINT;
BEGIN
  FOR r IN
    SELECT * FROM dq_rule_catalog WHERE is_active
    ORDER BY target_stage, rule_id
  LOOP
    -- Total records scanned = whole batch size for this stage
    IF r.target_stage = 'INGEST' THEN
      SELECT COUNT(*) INTO total_cnt FROM stg_price_offer WHERE source_batch_id = p_batch_id;
    ELSE
      SELECT COUNT(*) INTO total_cnt FROM fact_price_offer WHERE source_batch_id = p_batch_id;
    END IF;

    -- Failed records = rows returned by the check function
    EXECUTE format('SELECT COUNT(*) FROM %I($1)', r.check_function_name)
      INTO failed_cnt USING p_batch_id;

    -- Write aggregate row to dq_output
    INSERT INTO dq_output (rule_id, rule_name, rule_category, severity, run_ts,
                           total_records, failed_records, pass_rate, source_batch_id)
    VALUES (r.rule_id, r.rule_name, r.rule_category, r.severity, NOW(),
            total_cnt, failed_cnt,
            CASE WHEN total_cnt > 0
                 THEN ROUND(1.0 - failed_cnt::NUMERIC / total_cnt, 4)
                 ELSE 1.0 END,
            p_batch_id)
    RETURNING dq_run_id INTO run_id;

    -- If any failures, expand them into dq_bad_records
    IF failed_cnt > 0 THEN
      EXECUTE format($f$
        INSERT INTO dq_bad_records (
          source_batch_id, dq_run_id, raw_payload, rule_id,
          failed_field, error_message, severity
        )
        SELECT $1, $2, raw_payload, $3, failed_field, error_message, $4
        FROM %I($1)
      $f$, r.check_function_name)
      USING p_batch_id, run_id, r.rule_id, r.severity;
    END IF;

    rule_id        := r.rule_id;
    total_records  := total_cnt;
    failed_records := failed_cnt;
    pass_rate      := CASE WHEN total_cnt > 0
                           THEN ROUND(1.0 - failed_cnt::NUMERIC / total_cnt, 4)
                           ELSE 1.0 END;
    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- USAGE
-- ============================================================================
-- After each /load-data batch:
--
--   SELECT * FROM dq_run_batch('batch-uuid-here');
--
-- This returns one row per rule showing pass rate, and populates:
--   * dq_output        — one row per (rule, run)
--   * dq_bad_records   — one row per violating record (with raw payload)
--
-- Business users triage via:
--   SELECT * FROM dq_bad_records WHERE status = 'NEW' AND severity = 'HIGH'
--   ORDER BY detected_at DESC;
-- ============================================================================
