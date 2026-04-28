-- ============================================================================
-- demo_queries.sql · 面试现场 SQL 查询备忘录
-- ============================================================================
-- 用法:在 VSCode Database Client 中打开此文件,演讲时按章节
-- 选中需要的 SQL → 右键 Run Selected SQL(或扩展自带快捷键)
-- ============================================================================


-- ============================================================================
-- §0 · 飞行前检查(Demo 开始前自查)
-- ============================================================================

-- 0.1  Partner 数应为 2
SELECT COUNT(*) AS partners FROM dim_partner;

-- 0.2  DQ 规则三阶段分布应为 INGEST=10, PRE_FACT=3, SEMANTIC=6
SELECT target_stage, COUNT(*) AS rule_count
FROM dq_rule_catalog WHERE is_active
GROUP BY 1 ORDER BY 1;

-- 0.3  当前 fact 行数(基线状态应为 ~4174)
SELECT 'stg_price_offer'           AS t, COUNT(*) FROM stg_price_offer
UNION ALL SELECT 'fact_price_offer',          COUNT(*) FROM fact_price_offer
UNION ALL SELECT 'fact_partner_price_history', COUNT(*) FROM fact_partner_price_history
UNION ALL SELECT 'dq_bad_records',             COUNT(*) FROM dq_bad_records;


-- ============================================================================
-- §3 · 端到端入库 Demo
-- ============================================================================

-- 3.0  执行 curl 上传 Partner A.csv 后,把返回的 source_batch_id 填到这里
--      所有后续查询都用这个变量
SET demo.batch = '00000000-0000-0000-0000-000000000000';   -- ← 替换成真实 UUID


-- 3.0a 用法说明:
-- 在 VSCode Database Client 中,变量替换方式:
--   方法1:把下面所有 :'BATCH' 换成真实 UUID 字符串
--   方法2:用 current_setting('demo.batch') 引用上面 SET 的变量
-- 下面的查询用方法 1 风格写,你直接 Find & Replace 把 <BATCH_ID> 换掉

-- 3.1  本批次每阶段每规则的 pass-rate
SELECT c.target_stage,
       o.rule_id,
       o.failed_records,
       o.pass_rate
FROM dq_output o
JOIN dq_rule_catalog c USING (rule_id)
WHERE source_batch_id = '<BATCH_ID>'
ORDER BY c.target_stage, o.rule_id;


-- 3.2  流水线漏斗:staged → in_fact → flagged
SELECT
  (SELECT COUNT(*) FROM stg_price_offer  WHERE source_batch_id = '<BATCH_ID>') AS staged,
  (SELECT COUNT(*) FROM fact_price_offer WHERE source_batch_id = '<BATCH_ID>') AS in_fact,
  (SELECT COUNT(*) FROM dq_bad_records   WHERE source_batch_id = '<BATCH_ID>') AS flagged;


-- 3.3  Partner B 的 NZ 故事 — 在 dq_bad_records 里能看到 154 行原始 NZ 数据
--      (注:当前国家解析器已修复,这是历史数据。Demo 时用作"DQ 闭环"的真实案例)
SELECT raw_payload->>'COUNTRY_VAL' AS raw_country,
       COUNT(*) AS row_count
FROM dq_bad_records
WHERE rule_id = 'DQ_FMT_001'
GROUP BY 1
ORDER BY 2 DESC;


-- ============================================================================
-- §4 · 三阶段 DQ 深入讲解
-- ============================================================================

-- 4.1  按阶段 + 严重度 列出全部 13 条规则
SELECT target_stage,
       severity,
       rule_id,
       rule_name,
       check_function_name
FROM dq_rule_catalog
WHERE is_active
ORDER BY
  CASE target_stage
    WHEN 'INGEST'   THEN 1
    WHEN 'PRE_FACT' THEN 2
    WHEN 'SEMANTIC' THEN 3
  END,
  severity, rule_id;


-- 4.2  PRE_FACT gate 真的拦下来的例子:DQ_HARM_002(harmonise 完全匹配不上)
SELECT rule_id,
       raw_payload->>'PRODUCT_NAME_VAL' AS raw_product_name,
       error_message,
       severity,
       status
FROM dq_bad_records
WHERE rule_id = 'DQ_HARM_002'
LIMIT 5;


-- 4.3  SEMANTIC 阶段的软信号示例:DQ_HARM_001(harmonise 低置信)
--      展示业务审核员需要补 Reference 字典的真实信号
SELECT rule_id,
       error_message,
       raw_payload->>'PRODUCT_NAME_VAL' AS raw_product,
       severity
FROM dq_bad_records
WHERE rule_id = 'DQ_HARM_001'
LIMIT 5;


-- 4.4  按 rule_id 看违规分布(展示数据治理覆盖面)
SELECT rule_id, COUNT(*) AS violations
FROM dq_bad_records
WHERE source_batch_id = '<BATCH_ID>'
GROUP BY rule_id
ORDER BY violations DESC;


-- ============================================================================
-- §5 · 异常检测 Demo(可选注入合成异常)
-- ============================================================================

-- 5.1  注入合成基线 — 给 product_model_id=200, AU 加 5 行历史价(均 ~$1500)
INSERT INTO fact_partner_price_history
  (product_model_id, partner_id, country_code, payment_type, currency_code,
   effective_total_local, effective_total_usd, valid_from_date, valid_to_date)
VALUES
  (200, 1, 'AU', 'FULL', 'AUD', 2300, 1500, '2025-09-25', '2025-09-29'),
  (200, 1, 'AU', 'FULL', 'AUD', 2310, 1505, '2025-09-30', '2025-10-04'),
  (200, 1, 'AU', 'FULL', 'AUD', 2295, 1495, '2025-10-05', '2025-10-09'),
  (200, 1, 'AU', 'FULL', 'AUD', 2308, 1502, '2025-10-10', '2025-10-13'),
  (200, 1, 'AU', 'FULL', 'AUD', 2305, 1500, '2025-10-14', '2025-10-19');


-- 5.2  把某条 fact_price_offer 价格抬高 30%(明显异常)
UPDATE fact_price_offer
SET effective_total_usd  = 1950,
    effective_total_local = 3000
WHERE product_model_id = 200
  AND partner_id = 1
  AND country_code = 'AU'
  AND offer_id = (
    SELECT offer_id FROM fact_price_offer
    WHERE product_model_id = 200 AND partner_id = 1 AND country_code = 'AU'
    LIMIT 1
  );


-- 5.3  Demo 完毕清理 — 删掉合成基线行
DELETE FROM fact_partner_price_history
WHERE product_model_id = 200
  AND partner_id = 1
  AND country_code = 'AU'
  AND valid_from_date IN (
    '2025-09-25', '2025-09-30', '2025-10-05', '2025-10-10', '2025-10-14'
  );


-- 5.4  Demo 完毕清理 — 把抬高的那条价格改回正常
UPDATE fact_price_offer
SET effective_total_usd   = 909.32,
    effective_total_local = 1399.00
WHERE product_model_id = 200
  AND partner_id = 1
  AND country_code = 'AU'
  AND effective_total_usd = 1950;


-- ============================================================================
-- §6 · 业务审核闭环(/bad-records)
-- ============================================================================

-- 6.1  待审核队列(状态 NEW)— 演讲时展示"业务能看到的视图"
SELECT bad_record_id,
       rule_id,
       severity,
       error_message,
       raw_payload->>'PRODUCT_NAME_VAL' AS product_raw,
       detected_at
FROM dq_bad_records
WHERE status = 'NEW'
ORDER BY severity DESC, detected_at DESC
LIMIT 10;


-- 6.2  按 status 看审核进度
SELECT status, COUNT(*) AS records
FROM dq_bad_records
GROUP BY status
ORDER BY 1;


-- ============================================================================
-- §7 · SCD-2 历史追踪展示(讲 temporal tracking 时用)
-- ============================================================================

-- 7.1  某产品的完整价格历史(展示 SCD-2 模型)
SELECT product_model_id,
       partner_id,
       country_code,
       payment_type,
       effective_total_usd,
       valid_from_date,
       valid_to_date,
       CASE WHEN valid_to_date IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END AS state
FROM fact_partner_price_history
WHERE product_model_id = 200    -- 替换成有更新历史的 product_model_id
ORDER BY partner_id, country_code, valid_from_date;


-- 7.2  当前所有 partner 的活跃价格(via v_partner_price_current 视图)
SELECT *
FROM v_partner_price_current
WHERE partner_count > 1                -- 只看跨 partner 的产品
ORDER BY price_spread_pct DESC NULLS LAST
LIMIT 10;


-- ============================================================================
-- §8 · 兜底重置(如果 Demo 现场需要重置数据)
-- ============================================================================

-- 8.1  仅清空近 5 分钟产生的 demo 数据(不动 baseline)
BEGIN;
DELETE FROM fact_payment_full_price WHERE offer_id IN (
  SELECT offer_id FROM fact_price_offer WHERE created_at > NOW() - INTERVAL '5 minutes'
);
DELETE FROM fact_payment_instalment WHERE offer_id IN (
  SELECT offer_id FROM fact_price_offer WHERE created_at > NOW() - INTERVAL '5 minutes'
);
DELETE FROM dq_bad_records WHERE detected_at > NOW() - INTERVAL '5 minutes';
DELETE FROM dq_output      WHERE run_ts      > NOW() - INTERVAL '5 minutes';
DELETE FROM fact_price_offer WHERE created_at > NOW() - INTERVAL '5 minutes';
DELETE FROM stg_price_offer  WHERE source_batch_id IN (
  SELECT DISTINCT source_batch_id FROM stg_price_offer ORDER BY 1 LIMIT 100
);
COMMIT;


-- 8.2  完全重建 baseline(只在多次重跑搞乱的情况下用,运行后需要重新 curl 上传)
-- 注:此操作会丢失全部数据。请确保你已经记下要保留的 source_batch_id。
/*
TRUNCATE TABLE
  fact_payment_full_price,
  fact_payment_instalment,
  fact_price_offer,
  fact_partner_price_history,
  fact_anomaly,
  stg_price_offer,
  dq_bad_records,
  dq_output,
  dws_partner_dq_per_batch
RESTART IDENTITY CASCADE;
*/
