-- =====================================================================
-- GOLD LAYER  (src/pipelines/transformation_pipeline/transformations/gold.sql)
-- Star schema over Silver, plus watchlist screening and a daily risk rollup.
-- Fully qualified names are used because the pipeline default schema is 'silver'.
-- All Gold objects are materialized views: joins and aggregates must be
-- recomputed when their inputs change (for example a late transaction
-- changes the totals for an earlier day).
-- =====================================================================


-- ---------------------------------------------------------------------
-- dim_customer: one row per customer VERSION (from the SCD2 table).
-- customer_sk is a deterministic hash of the business key plus the version
-- start, so it stays stable across refreshes (unlike row_number()).
-- valid_from is the join boundary. A customer's FIRST version is backdated
-- to 1900-01-01 so facts dated before the first record still resolve.
-- version_start_at keeps the real start of each version.
-- ssn is kept here on purpose: the governance phase applies a column mask.
-- ---------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dev_kyc.gold.dim_customer
COMMENT 'Customer dimension, SCD type 2. Join facts on customer_sk'
AS
SELECT
xxhash64(customer_id, CAST(__START_AT AS STRING))   AS customer_sk,
customer_id,
first_name,
last_name,
lower(trim(regexp_replace(regexp_replace(concat_ws(' ', first_name, last_name), '[^A-Za-z ]', ' '), ' +', ' '))) AS full_name_norm,
ssn,
dob,
address,
city,
state,
country,
risk_rating,
kyc_status,
  CASE
    WHEN __START_AT = MIN(__START_AT) OVER (PARTITION BY customer_id)
      THEN TIMESTAMP '1900-01-01 00:00:00'
    ELSE __START_AT
  END                                                                    AS valid_from,
  __END_AT                                                               AS valid_to,
  __START_AT                                                             AS version_start_at,
  (__END_AT IS NULL)                                                     AS is_current
FROM dev_kyc.silver.customers_scd2;

-- ---------------------------------------------------------------------
-- dim_date: one row per calendar day between the first and last transaction.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dev_kyc.gold.dim_date
COMMENT 'Calendar dimension covering the transaction date range.'
AS
WITH bounds AS (
  SELECT MIN(to_date(txn_ts)) AS d0, MAX(to_date(txn_ts)) AS d1
  FROM dev_kyc.silver.transactions
),
days AS (
  SELECT explode(sequence(d0, d1, INTERVAL 1 DAY)) AS full_date FROM bounds
)
SELECT
  CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
  full_date,
  year(full_date)                                 AS year,
  quarter(full_date)                              AS quarter,
  month(full_date)                                AS month,
  date_format(full_date, 'MMMM')                  AS month_name,
  day(full_date)                                  AS day_of_month,
  date_format(full_date, 'EEEE')                  AS day_name,
  dayofweek(full_date) IN (1, 7)                  AS is_weekend
FROM days;


-- ---------------------------------------------------------------------
-- dim_merchant: derived from the transactions, because the source has no
-- merchant id. In a client engagement, ask for a real merchant key.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dev_kyc.gold.dim_merchant
COMMENT 'Merchant dimension derived from merchant name and category.'
AS
SELECT DISTINCT
  xxhash64(merchant_name, merchant_category)                      AS merchant_sk,
  merchant_name,
  merchant_category,
  merchant_category IN ('CRYPTO', 'GAMBLING', 'MONEY_TRANSFER')   AS is_high_risk_category
FROM dev_kyc.silver.transactions;

-- ---------------------------------------------------------------------
-- fact_transactions: one row per transaction (grain of silver.transactions).
-- Each transaction is joined to the customer version that was valid at txn_ts
-- (point-in-time join).
-- customer_match_status makes orphans visible instead of hiding them: a
-- transaction whose customer has no record yet (a late-arriving dimension)
-- gets a NULL customer_sk and the status 'ORPHAN', and resolves on a later
-- refresh once the customer data arrives.
-- The country list is a stand-in for a governed reference table.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dev_kyc.gold.fact_transactions
COMMENT 'Transaction fact table with point-in-time customer key and AML flags.'
AS
SELECT
  t.txn_id,
  c.customer_sk,
  CAST(date_format(t.txn_ts, 'yyyyMMdd') AS INT)                  AS date_key,
  xxhash64(t.merchant_name, t.merchant_category)                  AS merchant_sk,
  t.customer_id,
  t.txn_ts,
  t.amount,
  t.currency,
  t.txn_type,
  t.channel,
  t.country                                                       AS txn_country,
  (t.txn_type = 'DEPOSIT' AND t.amount >= 9000 AND t.amount < 10000) AS is_near_threshold,
  (t.country IN ('IR', 'KP', 'SY', 'MM'))                         AS is_high_risk_country,
  CASE WHEN c.customer_sk IS NULL THEN 'ORPHAN' ELSE 'MATCHED' END AS customer_match_status,
  t.source_file,
  t.ingest_ts
FROM dev_kyc.silver.transactions t
LEFT JOIN dev_kyc.gold.dim_customer c
  ON  t.customer_id = c.customer_id
  AND t.txn_ts >= c.valid_from
  AND t.txn_ts <  COALESCE(c.valid_to, TIMESTAMP '9999-12-31 00:00:00');

-- ---------------------------------------------------------------------
-- watchlist_matches: POTENTIAL matches between current customers and the
-- watchlist on normalized name. Name-only matches are low confidence and
-- go to review. Customers who happen to share a name produce false positives.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dev_kyc.gold.watchlist_matches
COMMENT 'Potential watchlist matches by normalized name, pending analyst review.'
AS
SELECT
  c.customer_id,
  w.watchlist_id,
  w.list_source,
  'NAME_NORMALIZED_EXACT'                                         AS match_type,
  (c.country = w.country)                                         AS country_match,
  CASE WHEN c.country = w.country THEN 'MEDIUM' ELSE 'LOW' END   AS match_confidence,
  'PENDING_REVIEW'                                                AS review_status
FROM dev_kyc.gold.dim_customer c
JOIN dev_kyc.silver.watchlist w
  ON c.full_name_norm = w.full_name_norm
WHERE c.is_current;


-- ---------------------------------------------------------------------
-- customer_risk_daily: one row per customer per day with alert flags.
-- structuring_alert: 3 or more near-threshold deposits in a day.
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dev_kyc.gold.customer_risk_daily
COMMENT 'Daily per-customer activity and AML alert flags.'
AS
SELECT
  f.customer_id,
  f.date_key,
  d.full_date                                                     AS txn_date,
  COUNT(*)                                                        AS txn_count,
  SUM(f.amount)                                                   AS total_amount,
  MAX(f.amount)                                                   AS max_amount,
  SUM(CASE WHEN f.is_near_threshold THEN 1 ELSE 0 END)            AS near_threshold_count,
  SUM(CASE WHEN f.is_high_risk_country THEN 1 ELSE 0 END)         AS high_risk_country_count,
  SUM(CASE WHEN f.is_near_threshold THEN 1 ELSE 0 END) >= 3       AS structuring_alert,
  SUM(CASE WHEN f.is_high_risk_country THEN 1 ELSE 0 END) > 0     AS high_risk_geo_alert,
  COALESCE(MAX(m.match_count), 0) > 0                             AS has_potential_watchlist_match
FROM dev_kyc.gold.fact_transactions f
JOIN dev_kyc.gold.dim_date d
  ON f.date_key = d.date_key
LEFT JOIN (
  SELECT customer_id, COUNT(*) AS match_count
  FROM dev_kyc.gold.watchlist_matches
  GROUP BY customer_id
) m
  ON f.customer_id = m.customer_id
GROUP BY f.customer_id, f.date_key, d.full_date;
