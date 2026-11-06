-- =====================================================================
-- DASHBOARD DATASETS  (src/dashboards/dashboard_queries.sql)
-- One query per dashboard dataset. All read from the Gold layer only, so the
-- dashboard never touches Bronze or Silver (which hold unmasked SSNs).
-- Paste each query into its own dataset in the dashboard editor.
-- =====================================================================

-- 1. KPI tiles: overall activity and alert volume.
SELECT
  COUNT(*)                                                       AS transactions,
  SUM(amount)                                                    AS total_amount,
  SUM(CASE WHEN is_near_threshold THEN 1 ELSE 0 END)             AS near_threshold_txns,
  SUM(CASE WHEN is_high_risk_country THEN 1 ELSE 0 END)          AS high_risk_country_txns
FROM dev_kyc.gold.fact_transactions;

-- 2. Daily transaction volume and amount (line or bar chart).
SELECT
  d.full_date,
  COUNT(*)        AS txns,
  SUM(f.amount)   AS total_amount
FROM dev_kyc.gold.fact_transactions f
JOIN dev_kyc.gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;

-- 3. Alerts per day (stacked bar). Reads customer_risk_daily, which carries
--    the state row filter, so analyst viewers see only FL and GA customers
--    when the dashboard runs with viewer credentials.
SELECT
  txn_date,
  SUM(CASE WHEN structuring_alert   THEN 1 ELSE 0 END) AS structuring_alerts,
  SUM(CASE WHEN high_risk_geo_alert THEN 1 ELSE 0 END) AS high_risk_geo_alerts
FROM dev_kyc.gold.customer_risk_daily
GROUP BY txn_date
ORDER BY txn_date;

-- 4. Top customers by alert days (table), with their current risk rating.
--    No SSN or name columns on purpose.
SELECT
  r.customer_id,
  c.risk_rating,
  SUM(CASE WHEN r.structuring_alert   THEN 1 ELSE 0 END) AS structuring_days,
  SUM(CASE WHEN r.high_risk_geo_alert THEN 1 ELSE 0 END) AS high_risk_geo_days,
  SUM(r.total_amount)                                    AS total_amount
FROM dev_kyc.gold.customer_risk_daily r
LEFT JOIN dev_kyc.gold.dim_customer c
  ON r.customer_id = c.customer_id AND c.is_current
GROUP BY r.customer_id, c.risk_rating
HAVING structuring_days > 0 OR high_risk_geo_days > 0
ORDER BY structuring_days DESC, high_risk_geo_days DESC, total_amount DESC
LIMIT 20;

-- 4. Top customers by alert days (table), with their current risk rating.
--    No SSN or name columns on purpose.
SELECT
  r.customer_id,
  c.risk_rating,
  SUM(CASE WHEN r.structuring_alert   THEN 1 ELSE 0 END) AS structuring_days,
  SUM(CASE WHEN r.high_risk_geo_alert THEN 1 ELSE 0 END) AS high_risk_geo_days,
  SUM(r.total_amount)                                    AS total_amount
FROM dev_kyc.gold.customer_risk_daily r
LEFT JOIN dev_kyc.gold.dim_customer c
  ON r.customer_id = c.customer_id AND c.is_current
GROUP BY r.customer_id, c.risk_rating
HAVING structuring_days > 0 OR high_risk_geo_days > 0
ORDER BY structuring_days DESC, high_risk_geo_days DESC, total_amount DESC
LIMIT 20;

-- 5. Spend by merchant category, flagging high-risk categories (bar chart).
SELECT
  m.merchant_category,
  m.is_high_risk_category,
  COUNT(*)        AS txns,
  SUM(f.amount)   AS total_amount
FROM dev_kyc.gold.fact_transactions f
JOIN dev_kyc.gold.dim_merchant m ON f.merchant_sk = m.merchant_sk
GROUP BY m.merchant_category, m.is_high_risk_category
ORDER BY total_amount DESC;

-- 6. High-risk country activity (bar chart or table).
SELECT
  txn_country,
  COUNT(*)      AS txns,
  SUM(amount)   AS total_amount
FROM dev_kyc.gold.fact_transactions
WHERE is_high_risk_country
GROUP BY txn_country
ORDER BY total_amount DESC;

-- 7. Potential watchlist matches awaiting review (counter or pie chart).
SELECT
  match_confidence,
  review_status,
  COUNT(*) AS matches
FROM dev_kyc.gold.watchlist_matches
GROUP BY match_confidence, review_status;

-- 8. Current customers by risk rating (bar chart).
SELECT
  risk_rating,
  COUNT(*) AS customers
FROM dev_kyc.gold.dim_customer
WHERE is_current
GROUP BY risk_rating
ORDER BY customers DESC;
