-- =====================================================================
-- MONITORING QUERIES  (src/monitoring/monitoring_queries.sql)
-- Part A: queries to attach to SQL alerts (one value each, compared to a threshold).
-- Part B: datasets for an engineer-only pipeline health dashboard.
-- All read from dev_kyc.ops, created by monitoring_setup.sql.
-- =====================================================================


-- =====================  PART A: ALERT QUERIES  =====================

-- ALERT 1: transaction rejection rate in the latest completed transformation update.
-- Expect about 1 to 2 percent per rule with this generator. Trigger when value > 5.
WITH latest AS (
  SELECT update_id
  FROM dev_kyc.ops.pipeline_updates
  WHERE pipeline = 'transformation_pipeline' AND state = 'COMPLETED'
  ORDER BY finished_at DESC
  LIMIT 1
)
SELECT COALESCE(MAX(failed_pct), 0) AS max_failed_pct
FROM dev_kyc.ops.dq_metrics
WHERE update_id IN (SELECT update_id FROM latest)
  AND dataset LIKE '%transactions_valid';


-- ALERT 2: stale data. Trigger when value > 36 (hours) for a daily job.
SELECT MAX(hours_since_ingest) AS max_hours_since_ingest
FROM dev_kyc.ops.data_freshness;


-- ALERT 3: any failed pipeline update in the last 24 hours. Trigger when value > 0.
SELECT COUNT(*) AS failed_updates_24h
FROM dev_kyc.ops.pipeline_updates
WHERE state = 'FAILED'
  AND finished_at >= current_timestamp() - INTERVAL 1 DAY;


-- ALERT 4 (optional): orphan rate in the latest run. Orphans are normal until
-- customer data arrives, so use a loose threshold, for example value > 10.
SELECT ROUND(100.0 * orphan_txns / NULLIF(fact_txns, 0), 2) AS orphan_pct
FROM dev_kyc.ops.run_log
ORDER BY run_ts DESC
LIMIT 1;
