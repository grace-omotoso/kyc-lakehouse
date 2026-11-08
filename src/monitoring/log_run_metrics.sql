-- =====================================================================
-- JOB TASK: log_run_metrics  (src/jobs/log_run_metrics.sql)
-- Runs after validate_silver and validate_gold. Appends one row to
-- dev_kyc.ops.run_log so counts and freshness can be trended over time.
-- Create the table first with monitoring_setup.sql.
-- =====================================================================
INSERT INTO dev_kyc.ops.run_log (
  run_ts, bronze_txns, silver_valid_txns, quarantined_txns, silver_txns,
  fact_txns, orphan_txns, bronze_customers, scd2_rows, max_ingest_ts, max_txn_ts
)
SELECT
  current_timestamp(),
  (SELECT COUNT(*) FROM dev_kyc.bronze.transactions),
  (SELECT COUNT(*) FROM dev_kyc.silver.transactions_valid),
  (SELECT COUNT(*) FROM dev_kyc.silver.transactions_quarantine),
  (SELECT COUNT(*) FROM dev_kyc.silver.transactions),
  (SELECT COUNT(*) FROM dev_kyc.gold.fact_transactions),
  (SELECT COUNT(*) FROM dev_kyc.gold.fact_transactions WHERE customer_match_status = 'ORPHAN'),
  (SELECT COUNT(*) FROM dev_kyc.bronze.customers),
  (SELECT COUNT(*) FROM dev_kyc.silver.customers_scd2),
  (SELECT MAX(ingest_ts) FROM dev_kyc.bronze.transactions),
  (SELECT MAX(txn_ts) FROM dev_kyc.silver.transactions);
