-- =====================================================================
-- SILVER LAYER  (src/pipelines/transformation_pipeiline/transformations/silver.sql)
-- Reads Bronze, casts types, validates, dedupes, and tracks customer history.

-- =====================================================================


-- ---------------------------------------------------------------------
-- TRANSACTIONS
-- Step 1: type casting only. No rows are dropped here, so every Bronze row
-- reaches either the valid table or the quarantine table.
-- try_cast returns NULL for unparseable values instead of failing the run.
-- channel is selected explicitly: schema drift in Bronze reaches Silver
-- only because we chose to add it.
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE dev_kyc.silver.transactions_typed
COMMENT 'Bronze transactions with types cast and text standardized. No filtering.'
AS
SELECT
  txn_id,
  customer_id,
  try_cast(txn_ts AS TIMESTAMP)        AS txn_ts,
  try_cast(amount AS DECIMAL(12,2))    AS amount,
  upper(trim(currency))                AS currency,
  upper(trim(txn_type))                AS txn_type,
  merchant_name,
  upper(trim(merchant_category))       AS merchant_category,
  upper(trim(country))                 AS country,
  upper(trim(channel))                 AS channel,
  source_file,
  ingest_ts
FROM STREAM(dev_kyc.bronze.transactions);

-- Step 2: valid rows. Expectations drop failing rows and record pass/fail
-- counts in the pipeline event log (used for monitoring in a later phase).
CREATE OR REFRESH STREAMING TABLE dev_kyc.silver.transactions_valid(
    CONSTRAINT txn_id_present EXPECT(txn_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT amount_positive EXPECT(amount > 0) ON VIOLATION DROP ROW,
    CONSTRAINT txn_ts_valid EXPECT(txn_ts IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Transactions passing all expectations. May still contain duplicate txn_id'
AS SELECT * FROM STREAM(dev_kyc.silver.transactions_typed);

-- Step 3: quarantine. Same rules inverted, so rejected rows are kept and explained.
-- Keep this WHERE clause in sync with the constraints above.
CREATE OR REFRESH STREAMING TABLE dev_kyc.silver.transactions_quarantine
COMMENT 'Transactions that failed validation with the reason.'
AS
SELECT
*,
CASE 
  WHEN txn_id IS NULL THEN 'missing_txn_id'
  WHEN txn_ts IS NULL THEN 'invalid_txn_ts'
  WHEN amount IS NULL THEN 'invalid_amount'
  WHEN amount <= 0 THEN 'non_positive_amount'
  END AS reject_reason
  FROM STREAM(dev_kyc.silver.transactions_typed)
  WHERE txn_id IS NULL OR txn_ts IS NULL OR amount is NULL OR amount <= 0;

-- Step 4: Dedup. One row per txn_id; if the same id arrives again, the copy
-- with the later ingest_ts wins (SCD type 1 upsert).
CREATE OR REFRESH STREAMING TABLE dev_kyc.silver.transactions
COMMENT 'Clean, deduplicated transactions. One row per txn_id.';

CREATE FLOW transactions_dedup AS AUTO CDC INTO dev_kyc.silver.transactions
FROM STREAM(dev_kyc.silver.transactions_valid)
KEYS(txn_id)
SEQUENCE BY ingest_ts
STORED AS SCD TYPE 1;

-- ---------------------------------------------------------------------
-- CUSTOMERS
-- Typed feed first, then the same feed into two targets to contrast
-- SCD type 1 (current state only) and SCD type 2 (full history).
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE dev_kyc.silver.customers_typed
COMMENT 'Bronze customer change feed with types cast. One row per customer per change.'
AS
SELECT
  customer_id,
  first_name,
  last_name,
  ssn,
  try_cast(dob AS DATE)           AS dob,
  address,
  city,
  upper(trim(state))              AS state,
  upper(trim(country))            AS country,
  upper(trim(risk_rating))        AS risk_rating,
  upper(trim(kyc_status))         AS kyc_status,
  try_cast(updated_at AS TIMESTAMP) AS updated_at,
  source_file,
  ingest_ts
FROM STREAM(dev_kyc.bronze.customers);

-- SCD type 1: latest values only, one row per customer, overwritten on change.
CREATE OR REFRESH STREAMING TABLE dev_kyc.silver.customers_current
COMMENT 'SCD type 1: current state of each customer.';

CREATE FLOW customers_current_flow AS AUTO CDC INTO dev_kyc.silver.customers_current
FROM STREAM(dev_kyc.silver.customers_typed)
KEYS(customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;

-- SCD type 2: a new version row only when a tracked column really changes.
-- Changes to other columns (for example kyc_status) update the open row in place.
-- __START_AT and __END_AT are added by the pipeline; __END_AT IS NULL marks the current version.
CREATE OR REFRESH STREAMING TABLE dev_kyc.silver.customers_scd2
COMMENT 'SCD type 2: history of address and risk rating changes.';

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO dev_kyc.silver.customers_scd2
FROM STREAM(dev_kyc.silver.customers_typed)
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2
TRACK HISTORY ON address, city, state, risk_rating;










