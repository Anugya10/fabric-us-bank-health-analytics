-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "ed2d451f-7951-4e5c-83a4-2615447a7a78",
-- META       "default_lakehouse_name": "lh_bank",
-- META       "default_lakehouse_workspace_id": "e8e3a7f2-9a9b-4ab5-b74b-bc0933da2da8",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "ed2d451f-7951-4e5c-83a4-2615447a7a78"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TEMP VIEW raw_financials
USING json OPTIONS (path 'Files/bronze/fdic/financials', recursiveFileLookup 'true');

CREATE OR REPLACE TEMP VIEW raw_institutions
USING json OPTIONS (path 'Files/bronze/fdic/institutions');

CREATE OR REPLACE TEMP VIEW raw_failures
USING json OPTIONS (path 'Files/bronze/fdic/failures');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW fin_typed AS
SELECT
    CAST(CERT AS INT)                           AS cert,
    TO_DATE(CAST(REPDTE AS STRING), 'yyyyMMdd') AS report_date,
    UPPER(TRIM(STALP))                          AS state_code,
    CAST(ASSET   AS DOUBLE) * 1000              AS total_assets,
    CAST(DEP     AS DOUBLE) * 1000              AS total_deposits,
    CAST(LNLSNET AS DOUBLE) * 1000              AS net_loans,
    CAST(EQ      AS DOUBLE) * 1000              AS total_equity,
    CAST(NETINC  AS DOUBLE) * 1000              AS net_income_ytd,
    CAST(NCLNLS  AS DOUBLE) * 1000              AS noncurrent_loans,
    CAST(ROA  AS DOUBLE)                        AS roa_pct,
    CAST(ROE  AS DOUBLE)                        AS roe_pct,
    CAST(NIMY AS DOUBLE)                        AS nim_pct
FROM raw_financials;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW inst_clean AS
SELECT
    CAST(CERT AS INT)            AS cert,
    TRIM(NAME)                   AS bank_name,
    INITCAP(TRIM(CITY))          AS city,
    UPPER(TRIM(STALP))           AS state_code,
    TRIM(STNAME)                 AS state_name,
    CAST(ACTIVE AS INT) = 1      AS is_active,
    CAST(BKCLASS AS STRING)      AS charter_class,
    CASE WHEN ESTYMD LIKE '%/%' THEN TO_DATE(ESTYMD, 'M/d/yyyy')
         ELSE TO_DATE(LEFT(ESTYMD, 10), 'yyyy-MM-dd') END AS established_date,
    current_timestamp()          AS _loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY CERT ORDER BY CERT) AS rn
    FROM raw_institutions
    WHERE load_date = (SELECT MAX(load_date) FROM raw_institutions)
)
WHERE rn = 1 AND CERT IS NOT NULL;

CREATE OR REPLACE TABLE silver.institutions AS SELECT * FROM inst_clean;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW fin_clean AS
WITH dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cert, report_date ORDER BY cert) AS rn
    FROM fin_typed
    WHERE cert IS NOT NULL AND report_date IS NOT NULL
),
with_prev AS (
    SELECT *,
        LAG(net_income_ytd)       OVER (PARTITION BY cert, YEAR(report_date) ORDER BY report_date) AS prev_ytd,
        LAG(QUARTER(report_date)) OVER (PARTITION BY cert, YEAR(report_date) ORDER BY report_date) AS prev_qtr
    FROM dedup
    WHERE rn = 1
)
SELECT
    cert, report_date, state_code,
    total_assets, total_deposits, net_loans, total_equity, noncurrent_loans,
    roa_pct, roe_pct, nim_pct, net_income_ytd,
    CASE WHEN QUARTER(report_date) = 1            THEN net_income_ytd
         WHEN prev_qtr = QUARTER(report_date) - 1 THEN net_income_ytd - prev_ytd
    END                  AS net_income_qtr,
    current_timestamp()  AS _loaded_at
FROM with_prev;

CREATE OR REPLACE TABLE silver.bank_financials AS SELECT * FROM fin_clean;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW fail_clean AS
SELECT
    CAST(CERT AS INT)                               AS cert,
    TRIM(NAME)                                      AS bank_name,
    TRIM(CITYST)                                    AS city_state,
    UPPER(TRIM(SUBSTRING_INDEX(CITYST, ',', -1)))   AS state_code,
    CASE WHEN FAILDATE LIKE '%/%' THEN TO_DATE(FAILDATE, 'M/d/yyyy')
         ELSE TO_DATE(LEFT(FAILDATE, 10), 'yyyy-MM-dd') END AS fail_date,
    CAST(QBFASSET AS DOUBLE) * 1000                 AS assets_at_failure,
    CAST(COST     AS DOUBLE) * 1000                 AS estimated_cost,
    current_timestamp()                             AS _loaded_at
FROM raw_failures
WHERE load_date = (SELECT MAX(load_date) FROM raw_failures);

CREATE OR REPLACE TABLE silver.failures AS SELECT * FROM fail_clean;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW dq_checks AS
SELECT 'bank_financials' AS table_name, 'keys_not_null' AS rule,
       COUNT_IF(cert IS NULL OR report_date IS NULL) AS failed_rows, COUNT(*) AS total_rows FROM fin_typed
UNION ALL SELECT 'bank_financials', 'pk_unique_cert_quarter',
       COUNT(*) - COUNT(DISTINCT cert, report_date), COUNT(*) FROM fin_typed
UNION ALL SELECT 'bank_financials', 'assets_not_negative',     COUNT_IF(total_assets < 0), COUNT(*) FROM fin_typed
UNION ALL SELECT 'bank_financials', 'equity_not_above_assets', COUNT_IF(total_equity > total_assets), COUNT(*) FROM fin_typed
UNION ALL SELECT 'bank_financials', 'state_code_valid',
       COUNT_IF(state_code IS NULL OR LENGTH(state_code) <> 2), COUNT(*) FROM fin_typed
UNION ALL SELECT 'bank_financials', 'cert_exists_in_institutions',
       (SELECT COUNT(*) FROM fin_typed f LEFT ANTI JOIN inst_clean i ON f.cert = i.cert),
       (SELECT COUNT(*) FROM fin_typed)
UNION ALL SELECT 'failures', 'fail_date_parsed', COUNT_IF(fail_date IS NULL), COUNT(*) FROM fail_clean;

-- Save this run's results (latest run; each pipeline run's history stays in the Monitor hub)
CREATE OR REPLACE TABLE silver.dq_log AS
SELECT current_timestamp() AS run_ts, table_name, rule, failed_rows, total_rows,
       CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM dq_checks;

SELECT * FROM dq_checks;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Stops the run if any row is missing its bank id or quarter date
SELECT IF(failed_rows > 0, raise_error('Critical DQ failure: null keys in bank_financials'), 'OK') AS critical_check
FROM dq_checks WHERE rule = 'keys_not_null';

SELECT 'institutions' AS table_name, COUNT(*) AS row_count FROM inst_clean
UNION ALL SELECT 'bank_financials', COUNT(*) FROM fin_clean
UNION ALL SELECT 'failures',        COUNT(*) FROM fail_clean;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
