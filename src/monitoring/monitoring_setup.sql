-- =====================================================================
-- MONITORING SETUP  (src/monitoring/monitoring_setup.sql)
-- Run in the SQL editor (not in a pipeline). Creates the objects in dev_kyc.ops.
--
-- PREREQUISITE: publish each pipeline's event log to a table, in the pipeline
-- settings (look for an option to publish the event log to the metastore),
-- using these names:
--     ingestion_pipeline       -> dev_kyc.ops.event_log_ingestion
--     transformation_pipeline  -> dev_kyc.ops.event_log_transformation
-- Run each pipeline once after changing the setting so the tables exist.
-- (The event_log() function is an alternative, but it only works for pipelines
-- in legacy publishing mode and only for the pipeline owner.)
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. run_log: one row per job run, written by the log_run_metrics.sql task.
--    A history of counts, so you can see trends and not only pass or fail.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev_kyc.ops.run_log (
  run_ts             TIMESTAMP,
  bronze_txns        BIGINT,
  silver_valid_txns  BIGINT,
  quarantined_txns   BIGINT,
  silver_txns        BIGINT,
  fact_txns          BIGINT,
  orphan_txns        BIGINT,
  bronze_customers   BIGINT,
  scd2_rows          BIGINT,
  max_ingest_ts      TIMESTAMP,
  max_txn_ts         TIMESTAMP
)
COMMENT 'Row counts and freshness captured after each job run.';


-- ---------------------------------------------------------------------
-- 2. dq_metrics: expectation pass and fail counts per pipeline update,
--    parsed from the event log.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW dev_kyc.ops.dq_metrics AS
WITH parsed AS (
  SELECT
    origin.update_id AS update_id,
    timestamp,
    explode(
      from_json(
        details:flow_progress.data_quality.expectations,
        'array<struct<name:string, dataset:string, passed_records:int, failed_records:int>>'
      )
    ) AS e
  FROM dev_kyc.ops.event_log_transformation
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
SELECT
  update_id,
  MIN(timestamp)                                                     AS first_event_ts,
  e.dataset                                                          AS dataset,
  e.name                                                             AS expectation,
  SUM(e.passed_records)                                              AS passed_records,
  SUM(e.failed_records)                                              AS failed_records,
  ROUND(100.0 * SUM(e.failed_records)
        / NULLIF(SUM(e.passed_records) + SUM(e.failed_records), 0), 2) AS failed_pct
FROM parsed
GROUP BY update_id, e.dataset, e.name;


-- ---------------------------------------------------------------------
-- 3. pipeline_updates: final state of every update of both pipelines.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW dev_kyc.ops.pipeline_updates AS
SELECT 'ingestion_pipeline' AS pipeline,
       origin.update_id     AS update_id,
       timestamp            AS finished_at,
       details:update_progress.state AS state
FROM dev_kyc.ops.event_log_ingestion
WHERE event_type = 'update_progress'
  AND details:update_progress.state IN ('COMPLETED', 'FAILED', 'CANCELED')
UNION ALL
SELECT 'transformation_pipeline',
       origin.update_id,
       timestamp,
       details:update_progress.state
FROM dev_kyc.ops.event_log_transformation
WHERE event_type = 'update_progress'
  AND details:update_progress.state IN ('COMPLETED', 'FAILED', 'CANCELED');


-- ---------------------------------------------------------------------
-- 4. data_freshness: how long since each layer last loaded data.
--    Freshness is judged on ingest_ts (load time). txn_ts is event time, and
--    this project's data is synthetic, so it says nothing about lateness.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW dev_kyc.ops.data_freshness AS
SELECT
  dataset,
  latest_ingest_ts,
  latest_event_ts,
  ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(latest_ingest_ts)) / 3600, 1) AS hours_since_ingest
FROM (
  SELECT 'bronze.transactions' AS dataset,
         MAX(ingest_ts) AS latest_ingest_ts,
         MAX(try_cast(txn_ts AS TIMESTAMP)) AS latest_event_ts
  FROM dev_kyc.bronze.transactions
  UNION ALL
  SELECT 'silver.transactions', MAX(ingest_ts), MAX(txn_ts)
  FROM dev_kyc.silver.transactions
  UNION ALL
  SELECT 'gold.fact_transactions', MAX(ingest_ts), MAX(txn_ts)
  FROM dev_kyc.gold.fact_transactions
);


-- ---------------------------------------------------------------------
-- 5. Cost and access, from Databricks system tables.
--    These need access to the system schemas; an account admin may have to
--    enable them. Run DESCRIBE on each system table first to confirm the
--    column names below exist in your workspace.
-- ---------------------------------------------------------------------
-- DESCRIBE TABLE system.billing.usage;
-- DESCRIBE TABLE system.access.table_lineage;

CREATE OR REPLACE VIEW dev_kyc.ops.cost_daily AS
SELECT
  usage_date,
  sku_name,
  usage_unit,
  SUM(usage_quantity) AS usage_quantity
FROM system.billing.usage
WHERE usage_date >= date_sub(current_date(), 30)
GROUP BY usage_date, sku_name, usage_unit;

CREATE OR REPLACE VIEW dev_kyc.ops.cost_by_job AS
SELECT
  usage_date,
  usage_metadata.job_id AS job_id,
  SUM(usage_quantity)   AS usage_quantity
FROM system.billing.usage
WHERE usage_date >= date_sub(current_date(), 30)
  AND usage_metadata.job_id IS NOT NULL
GROUP BY usage_date, usage_metadata.job_id;

-- Who has been reading Gold, from lineage records (a proxy for access;
-- system.access.audit is the full audit trail).
CREATE OR REPLACE VIEW dev_kyc.ops.gold_readers AS
SELECT
  created_by,
  source_table_full_name,
  COUNT(*)        AS read_events,
  MAX(event_time) AS last_read_at
FROM system.access.table_lineage
WHERE source_table_full_name LIKE 'dev_kyc.gold.%'
  AND event_time >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY created_by, source_table_full_name;

-- To explore the audit log before building anything on it:
-- SELECT DISTINCT service_name, action_name
-- FROM system.access.audit
-- WHERE event_date >= date_sub(current_date(), 7)
-- ORDER BY service_name, action_name;