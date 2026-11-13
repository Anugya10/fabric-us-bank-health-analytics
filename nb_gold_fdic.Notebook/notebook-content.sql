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

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TEMP VIEW s_fin  USING delta OPTIONS (path 'Tables/silver/bank_financials');
CREATE OR REPLACE TEMP VIEW s_inst USING delta OPTIONS (path 'Tables/silver/institutions');
CREATE OR REPLACE TEMP VIEW s_fail USING delta OPTIONS (path 'Tables/silver/failures');

SELECT (SELECT COUNT(*) FROM s_fin) AS fin_rows, (SELECT COUNT(*) FROM s_inst) AS inst_rows, (SELECT COUNT(*) FROM s_fail) AS fail_rows;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW v_dim_state AS
WITH codes AS (                                   -- every state code used anywhere
    SELECT state_code FROM s_inst
    UNION SELECT state_code FROM s_fin
    UNION SELECT state_code FROM s_fail
),
names AS (
    SELECT state_code, MAX(state_name) AS state_name FROM s_inst GROUP BY state_code
)
SELECT
    c.state_code,
    COALESCE(n.state_name, c.state_code) AS state_name,
    CASE
        WHEN c.state_code IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
        WHEN c.state_code IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
        WHEN c.state_code IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
        WHEN c.state_code IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
        ELSE 'Territories/Other'
    END AS region
FROM codes c
LEFT JOIN names n ON c.state_code = n.state_code
WHERE c.state_code IS NOT NULL AND LENGTH(c.state_code) = 2;

CREATE OR REPLACE TABLE gold.dim_state AS SELECT * FROM v_dim_state;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW v_dim_date AS
WITH b AS (SELECT COALESCE(TRUNC(MIN(fail_date), 'YEAR'), DATE'2019-01-01') AS start_d FROM s_fail),
     e AS (SELECT MAX(report_date) AS end_d FROM s_fin),
     starts AS (                                  -- first day of every quarter
         SELECT EXPLODE(SEQUENCE(b.start_d, e.end_d, INTERVAL 3 MONTH)) AS q_start
         FROM b CROSS JOIN e
     )
SELECT
    LAST_DAY(ADD_MONTHS(q_start, 2))              AS date_key,       -- quarter-end date (joins to facts)
    YEAR(q_start)                                 AS year,
    QUARTER(q_start)                              AS quarter,
    CONCAT(YEAR(q_start), ' Q', QUARTER(q_start)) AS quarter_label,  -- "2024 Q2"
    YEAR(q_start) * 10 + QUARTER(q_start)         AS quarter_sort,   -- 20242, used to sort labels
    LAST_DAY(ADD_MONTHS(q_start, 2)) = e.end_d    AS is_latest_quarter
FROM starts CROSS JOIN e;

CREATE OR REPLACE TABLE gold.dim_date AS SELECT * FROM v_dim_date;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW v_dim_bank AS
WITH latest AS (                                  -- each bank's most recent total assets
    SELECT cert, total_assets FROM (
        SELECT cert, total_assets,
               ROW_NUMBER() OVER (PARTITION BY cert ORDER BY report_date DESC) AS rn
        FROM s_fin)
    WHERE rn = 1
),
failed AS (SELECT DISTINCT cert FROM s_fail WHERE cert IS NOT NULL)
SELECT
    i.cert, i.bank_name, i.city, i.state_code, i.charter_class,
    CASE i.charter_class
        WHEN 'N'  THEN 'National bank'
        WHEN 'SM' THEN 'State member bank'
        WHEN 'NM' THEN 'State non-member bank'
        WHEN 'SB' THEN 'Savings bank'
        WHEN 'SA' THEN 'Savings association'
        WHEN 'SL' THEN 'Savings and loan'
        ELSE 'Other'
    END                                           AS charter_type,
    i.is_active,
    i.established_date,
    l.total_assets                                AS latest_total_assets,
    CASE
        WHEN l.total_assets IS NULL  THEN '0. No data since 2019'
        WHEN l.total_assets < 1e8    THEN '1. Under $100M'
        WHEN l.total_assets < 1e9    THEN '2. $100M - $1B'
        WHEN l.total_assets < 1e10   THEN '3. $1B - $10B'
        WHEN l.total_assets < 1e11   THEN '4. $10B - $100B'
        ELSE                              '5. Over $100B'
    END                                           AS size_band,
    f.cert IS NOT NULL                            AS has_failed
FROM s_inst i
LEFT JOIN latest l ON i.cert = l.cert
LEFT JOIN failed f ON i.cert = f.cert;

CREATE OR REPLACE TABLE gold.dim_bank AS SELECT * FROM v_dim_bank;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW v_fact_bank_quarter AS
SELECT
    cert,                                          -- → dim_bank
    report_date AS date_key,                       -- → dim_date
    state_code,                                    -- → dim_state
    total_assets, total_deposits, net_loans, total_equity, noncurrent_loans,
    net_income_ytd, net_income_qtr,
    roa_pct, roe_pct, nim_pct,
    noncurrent_loans / NULLIF(net_loans, 0)      AS noncurrent_loan_ratio,  -- bad loans share
    total_equity     / NULLIF(total_assets, 0)   AS equity_to_assets,       -- safety cushion
    net_loans        / NULLIF(total_deposits, 0) AS loans_to_deposits       -- lending intensity
FROM s_fin;

CREATE OR REPLACE TABLE gold.fact_bank_quarter AS SELECT * FROM v_fact_bank_quarter;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TEMP VIEW v_fact_bank_failure AS
SELECT
    cert,
    bank_name,                                                   -- kept: some very old failures have no bank record
    fail_date,
    LAST_DAY(ADD_MONTHS(TRUNC(fail_date, 'QUARTER'), 2)) AS date_key,   -- quarter end → dim_date
    state_code,
    assets_at_failure,
    estimated_cost,
    estimated_cost / NULLIF(assets_at_failure, 0)        AS cost_to_assets_ratio
FROM s_fail
WHERE fail_date IS NOT NULL;

CREATE OR REPLACE TABLE gold.fact_bank_failure AS SELECT * FROM v_fact_bank_failure;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Row counts
SELECT 'dim_state' AS table_name, COUNT(*) AS row_count FROM v_dim_state
UNION ALL SELECT 'dim_date',          COUNT(*) FROM v_dim_date
UNION ALL SELECT 'dim_bank',          COUNT(*) FROM v_dim_bank
UNION ALL SELECT 'fact_bank_quarter', COUNT(*) FROM v_fact_bank_quarter
UNION ALL SELECT 'fact_bank_failure', COUNT(*) FROM v_fact_bank_failure;

-- Orphan check: fact rows whose key has no match in the dimension (should be 0)
SELECT 'quarter -> bank'  AS relationship, COUNT(*) AS orphans FROM v_fact_bank_quarter f LEFT ANTI JOIN v_dim_bank  d ON f.cert = d.cert
UNION ALL SELECT 'quarter -> date',  COUNT(*) FROM v_fact_bank_quarter f LEFT ANTI JOIN v_dim_date  d ON f.date_key = d.date_key
UNION ALL SELECT 'quarter -> state', COUNT(*) FROM v_fact_bank_quarter f LEFT ANTI JOIN v_dim_state d ON f.state_code = d.state_code
UNION ALL SELECT 'failure -> date',  COUNT(*) FROM v_fact_bank_failure f LEFT ANTI JOIN v_dim_date  d ON f.date_key = d.date_key
UNION ALL SELECT 'failure -> state', COUNT(*) FROM v_fact_bank_failure f LEFT ANTI JOIN v_dim_state d ON f.state_code = d.state_code;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
