-- ============================================================================
-- rules_split.sql — splits dq_run_batch() into three stage-aware variants
-- ============================================================================
-- Companion to rules.sql. Adds:
--
--   dq_run_batch_ingest(batch_id)
--     → runs ALL active rules where target_stage = 'INGEST'
--     → meant to run on raw stg_price_offer BEFORE harmonise
--       (catches parse / format / required-field failures)
--
--   dq_run_batch_pre_fact(batch_id)
--     → runs ALL active rules where target_stage = 'PRE_FACT'
--     → meant to run on enriched stg_price_offer AFTER harmonise but
--       BEFORE inserting into fact_price_offer. Failing rows do NOT enter
--       fact (HIGH-severity gate against factual errors).
--
--   dq_run_batch_semantic(batch_id)
--     → runs ALL active rules where target_stage = 'SEMANTIC'
--     → meant to run on fact_price_offer AFTER load (soft signals only —
--       low-confidence harmonise + category sanity bounds).
--       Failing rows STAY in fact, just flagged in dq_bad_records for triage.
--
-- All three write to dq_output and dq_bad_records using the same patterns
-- as the original dq_run_batch().
-- ============================================================================

CREATE OR REPLACE FUNCTION dq_run_batch_stage(p_batch_id UUID, p_stage VARCHAR)
RETURNS TABLE (rule_id VARCHAR, total_records INT, failed_records INT, pass_rate NUMERIC) AS $$
DECLARE
  r          RECORD;
  total_cnt  INT;
  failed_cnt INT;
  run_id     BIGINT;
BEGIN
  FOR r IN
    SELECT * FROM dq_rule_catalog
    WHERE is_active AND target_stage = p_stage
    ORDER BY rule_id
  LOOP
    -- Choose the right scan target. INGEST and PRE_FACT both scan stg.
    IF r.target_stage IN ('INGEST', 'PRE_FACT') THEN
      SELECT COUNT(*) INTO total_cnt FROM stg_price_offer WHERE source_batch_id = p_batch_id;
    ELSE
      SELECT COUNT(*) INTO total_cnt FROM fact_price_offer WHERE source_batch_id = p_batch_id;
    END IF;

    EXECUTE format('SELECT COUNT(*) FROM %I($1)', r.check_function_name)
      INTO failed_cnt USING p_batch_id;

    INSERT INTO dq_output (rule_id, rule_name, rule_category, severity, run_ts,
                           total_records, failed_records, pass_rate, source_batch_id)
    VALUES (r.rule_id, r.rule_name, r.rule_category, r.severity, NOW(),
            total_cnt, failed_cnt,
            CASE WHEN total_cnt > 0
                 THEN ROUND(1.0 - failed_cnt::NUMERIC / total_cnt, 4)
                 ELSE 1.0 END,
            p_batch_id)
    RETURNING dq_run_id INTO run_id;

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


CREATE OR REPLACE FUNCTION dq_run_batch_ingest(p_batch_id UUID)
RETURNS TABLE (rule_id VARCHAR, total_records INT, failed_records INT, pass_rate NUMERIC) AS $$
  SELECT * FROM dq_run_batch_stage(p_batch_id, 'INGEST');
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION dq_run_batch_pre_fact(p_batch_id UUID)
RETURNS TABLE (rule_id VARCHAR, total_records INT, failed_records INT, pass_rate NUMERIC) AS $$
  SELECT * FROM dq_run_batch_stage(p_batch_id, 'PRE_FACT');
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION dq_run_batch_semantic(p_batch_id UUID)
RETURNS TABLE (rule_id VARCHAR, total_records INT, failed_records INT, pass_rate NUMERIC) AS $$
  SELECT * FROM dq_run_batch_stage(p_batch_id, 'SEMANTIC');
$$ LANGUAGE SQL;
