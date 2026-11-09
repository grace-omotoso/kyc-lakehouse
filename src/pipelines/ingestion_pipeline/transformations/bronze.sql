---------------- IMPORT BRONZE TABLES FROM S3 -------------------------------

---------------- TRANSACTIONS -----------------------------------------------
CREATE OR REFRESH STREAMING TABLE dev_kyc.bronze.transactions 
AS
SELECT *, _metadata.file_path as source_file, current_timestamp() as ingest_ts
FROM
STREAM read_files('s3://kyc-lakehouse-gracomot/landing/transactions/', format => 'json');


----------------- CUSTOMERS --------------------------------------------------
CREATE OR REFRESH STREAMING TABLE dev_kyc.bronze.customers
AS
SELECT *, _metadata.file_path as source_file, current_timestamp() as ingest_ts
FROM 
STREAM read_files('s3://kyc-lakehouse-gracomot/landing/customers/', format => 'csv', header => true);


--------------- WATCHLIST ----------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dev_kyc.bronze.watchlist
AS
SELECT *, _metadata.file_path as source_file, current_timestamp() as ingest_ts
FROM 
read_files('s3://kyc-lakehouse-gracomot/landing/watchlist/', format => 'csv', header => true);
