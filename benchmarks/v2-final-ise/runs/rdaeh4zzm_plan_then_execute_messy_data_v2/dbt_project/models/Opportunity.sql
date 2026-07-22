{{ config(materialized='table') }}

WITH opportunity_raw AS (
    SELECT * FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
account_id_map AS (
    SELECT TRIM(UPPER(id)) AS normalized_account_id, id AS account_id
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),
opportunity_enriched AS (
    SELECT 
        o.*,
        COALESCE(aim.account_id, NULL) AS resolved_account_id
    FROM opportunity_raw o
    LEFT JOIN account_id_map aim ON TRIM(UPPER(o.accountid)) = aim.normalized_account_id
)
SELECT
    TRIM(id) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'quali', 'qualifikation', 'in prüfung') THEN 'Qualification'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(closedate), 'YYYY-MM-DD')::TEXT
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(TRIM(closedate), 'YYYYMMDD')::TEXT
        WHEN closedate ~ '^\d+/\d+/\d{4}$' THEN TO_DATE(TRIM(closedate), 'M/D/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN TRIM(amount) IS NULL OR TRIM(LOWER(TRIM(amount))) IN ('none', '', 'null') THEN NULL
        ELSE CAST(
            REPLACE(
                REGEXP_REPLACE(
                    CASE 
                        -- European format: has both dots and commas; treat dot as thousands separator
                        WHEN REGEXP_REPLACE(amount, '[^0-9.,\-]', '', 'g') ~ '\d+\.\d{3},\d+' THEN 
                            REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^0-9.,\-]', '', 'g'), '\.', '')
                        ELSE REGEXP_REPLACE(amount, '[^0-9.,\-]', '', 'g')
                    END,
                 ',', '.')
         ::DOUBLE PRECISION
        )
    END AS "Amount",
    CASE 
        WHEN LOWER(TRIM(currencyisocode)) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(currencyisocode)) IN ('eur', '€', 'euro') THEN 'EUR'
        WHEN LOWER(TRIM(currencyisocode)) IN ('gbp', '£', 'pound') THEN 'GBP'
        WHEN LOWER(TRIM(currencyisocode)) IN ('chf') THEN 'CHF'
        ELSE UPPER(TRIM(currencyisocode))
    END AS "CurrencyIsoCode",
    resolved_account_id AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunity_enriched