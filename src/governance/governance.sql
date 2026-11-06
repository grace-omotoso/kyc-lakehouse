-- =====================================================================
-- GOVERNANCE  (run in the SQL editor, NOT in a pipeline)
--
-- Order of operations:
--   0. Create the groups in the account console, add yourself to
--      data_engineers and compliance, and assign the groups to this workspace.
--   1. Run PART 1 (functions) and PART 2 (grants) of this file.
--   2. Replace gold.sql in transformation_pipeline with the updated version
--      and run the pipeline (dim_customer now references the mask function).
--   3. Run PART 3 (tests).
-- =====================================================================


-- ---------------------------------------------------------------------
-- PART 1: functions (kept in the ops schema)
-- ---------------------------------------------------------------------

-- Column mask: compliance sees the full SSN, everyone else sees the last 4 digits.
CREATE OR REPLACE FUNCTION dev_kyc.ops.mask_ssn(ssn STRING)
  RETURN CASE
    WHEN is_account_group_member('compliance') THEN ssn
    ELSE concat('***-**-', right(ssn, 4))
  END;

-- Row filter: engineers and compliance see every row; analysts see only
-- customers in the southeast region (FL, GA). The region list is hardcoded for
-- this project. A mapping table is the production pattern (verify mapping-table
-- support in row filter functions in your workspace before relying on it).
CREATE OR REPLACE FUNCTION dev_kyc.ops.state_filter(row_state STRING)
  RETURN is_account_group_member('data_engineers')
      OR is_account_group_member('compliance')
      OR (is_account_group_member('analysts') AND row_state IN ('FL', 'GA'));


-- ---------------------------------------------------------------------
-- PART 2: grants
-- Principle: analysts and compliance read Gold only. Bronze and Silver hold
-- raw SSNs and are not masked, so access control is what protects them.
-- ---------------------------------------------------------------------

-- Everyone needs to reach the catalog.
GRANT USE CATALOG ON CATALOG dev_kyc TO `data_engineers`;
GRANT USE CATALOG ON CATALOG dev_kyc TO `compliance`;
GRANT USE CATALOG ON CATALOG dev_kyc TO `analysts`;

-- Engineers: full control of every layer, plus read access to the raw files.
GRANT ALL PRIVILEGES ON SCHEMA dev_kyc.bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA dev_kyc.silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA dev_kyc.gold   TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA dev_kyc.ops    TO `data_engineers`;
GRANT READ FILES ON EXTERNAL LOCATION kyc_lakehouse_loc TO `data_engineers`;

-- Compliance and analysts: read-only on Gold. SELECT at schema level also
-- covers Gold objects created later.
GRANT USE SCHEMA, SELECT ON SCHEMA dev_kyc.gold TO `compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA dev_kyc.gold TO `analysts`;

-- Review what is granted.
SHOW GRANTS ON SCHEMA dev_kyc.gold;
SHOW GRANTS ON SCHEMA dev_kyc.silver;