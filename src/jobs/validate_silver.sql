-- =====================================================================
-- SILVER QUALITY GATE  (src/jobs/validate_silver.sql)
-- Job task that runs after the Silver pipeline and before Gold is built.
-- assert_true fails the task when its condition is false, so a bad Silver
-- layer stops the job before it can feed Gold.
-- =====================================================================
SELECT
  assert_true(
    bronze_rows = valid_rows + quarantined_rows,
    'Silver gate: bronze transactions do not equal valid plus quarantined rows'
  ) AS reconciliation_ok,
  assert_true(
    silver_rows = distinct_ids,
    'Silver gate: duplicate txn_id values in silver.transactions'
  ) AS no_duplicate_txn_ids_ok,
  assert_true(
    silver_rows <= valid_rows,
    'Silver gate: silver.transactions has more rows than silver.transactions_valid'
  ) AS dedup_not_inflating_ok,
  assert_true(
    scd1_rows = distinct_customers,
    'Silver gate: customers_current is not one row per customer'
  ) AS customers_current_ok,
  assert_true(
    scd2_open_rows = distinct_customers,
    'Silver gate: customers_scd2 does not have exactly one open version per customer'
  ) AS customers_scd2_ok
FROM (
  SELECT
    (SELECT COUNT(*) FROM dev_kyc.bronze.transactions)                       AS bronze_rows,
    (SELECT COUNT(*) FROM dev_kyc.silver.transactions_valid)                 AS valid_rows,
    (SELECT COUNT(*) FROM dev_kyc.silver.transactions_quarantine)            AS quarantined_rows,
    (SELECT COUNT(*) FROM dev_kyc.silver.transactions)                       AS silver_rows,
    (SELECT COUNT(DISTINCT txn_id) FROM dev_kyc.silver.transactions)         AS distinct_ids,
    (SELECT COUNT(DISTINCT customer_id) FROM dev_kyc.bronze.customers)       AS distinct_customers,
    (SELECT COUNT(*) FROM dev_kyc.silver.customers_current)                  AS scd1_rows,
    (SELECT COUNT(*) FROM dev_kyc.silver.customers_scd2 WHERE __END_AT IS NULL) AS scd2_open_rows
);
