-- =====================================================================
-- GOLD QUALITY GATE  (src/jobs/validate_gold.sql)
-- Job task that runs after the Gold build and before the dashboard refresh.
-- assert_true returns NULL when its condition holds and raises an error when
-- it does not, so an all-NULL result means every check passed.
-- Orphan transactions (customer not loaded yet) are deliberately NOT asserted:
-- they are legitimate until the customer data arrives and are tracked as a
-- metric instead.
-- Coverage checks use LEFT ANTI JOIN instead of correlated NOT EXISTS
-- subqueries, which triggered an internal optimizer error.
-- =====================================================================
WITH counts AS (
  SELECT
    (SELECT COUNT(*) FROM dev_kyc.silver.transactions)                         AS silver_rows,
    (SELECT COUNT(*) FROM dev_kyc.gold.fact_transactions)                      AS fact_rows,
    (SELECT COUNT(DISTINCT txn_id) FROM dev_kyc.gold.fact_transactions)        AS fact_distinct_ids,
    (SELECT COUNT(*) FROM dev_kyc.gold.dim_customer WHERE is_current)          AS current_dim_rows,
    (SELECT COUNT(DISTINCT customer_id) FROM dev_kyc.silver.customers_current) AS distinct_customers
),
missing_dates AS (
  SELECT COUNT(*) AS n
  FROM dev_kyc.gold.fact_transactions f
  LEFT ANTI JOIN dev_kyc.gold.dim_date d
    ON f.date_key = d.date_key
),
missing_merchants AS (
  SELECT COUNT(*) AS n
  FROM dev_kyc.gold.fact_transactions f
  LEFT ANTI JOIN dev_kyc.gold.dim_merchant m
    ON f.merchant_sk = m.merchant_sk
)
SELECT
  assert_true(
    c.silver_rows = c.fact_rows,
    'Gold gate: fact_transactions row count differs from silver.transactions (grain broken)'
  ) AS grain_ok,
  assert_true(
    c.fact_rows = c.fact_distinct_ids,
    'Gold gate: duplicate txn_id values in fact_transactions'
  ) AS fact_unique_ok,
  assert_true(
    c.current_dim_rows = c.distinct_customers,
    'Gold gate: dim_customer does not have exactly one current row per customer'
  ) AS dim_customer_ok,
  assert_true(
    md.n = 0,
    'Gold gate: fact_transactions has date_key values missing from dim_date'
  ) AS dim_date_coverage_ok,
  assert_true(
    mm.n = 0,
    'Gold gate: fact_transactions has merchant_sk values missing from dim_merchant'
  ) AS dim_merchant_coverage_ok
FROM counts c
CROSS JOIN missing_dates md
CROSS JOIN missing_merchants mm;
