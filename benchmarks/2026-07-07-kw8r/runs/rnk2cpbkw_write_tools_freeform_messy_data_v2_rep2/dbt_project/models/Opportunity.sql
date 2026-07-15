{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),

transformed AS (
    SELECT
        id AS "Id",
        COALESCE(TRIM(INITCAP(name)), '') AS "Name",
        CASE
            WHEN LOWER(TRIM(stagename)) IN ('prospecting') THEN 'Prospecting'
            WHEN LOWER(TRIM(stagename)) IN ('qualification') THEN 'Qualification'
            WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'need analysis') THEN 'Needs Analysis'
            WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value prop') THEN 'Value Proposition'
            WHEN LOWER(TRIM(stagename)) IN ('identify decision makers', 'id. decision makers', 'identifying decision makers') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perception check') THEN 'Perception Analysis'
            WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal and price quote') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiations', 'negotiating') THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won') THEN 'Closed Won'
            WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",
        CASE
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
            WHEN closedate ~ '^\d{8}$' THEN SUBSTR(closedate, 1, 4) || '-' || SUBSTR(closedate, 5, 2) || '-' || SUBSTR(closedate, 7, 2)
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS "CloseDate",
        CASE
            -- European format: 1.234,56 (dot=thousands, comma=decimal)
            WHEN REGEXP_REPLACE(amount, '[\$\€£\s]', '') ~ '^\d{1,3}(\.\d{3})+,\d+$' THEN
                CAST(REGEXP_REPLACE(REGEXP_REPLACE(amount, '[\$\€£\s]', ''), '\.', '') AS DOUBLE PRECISION)
            -- Has comma but no dot-thousands pattern: 1.234,56 or 1234,56 - comma is decimal separator
            WHEN amount ~ '[,.].*\,' THEN
                CAST(REPLACE(REGEXP_REPLACE(amount, '[\$\€£\s]', ''), ',', '.') AS DOUBLE PRECISION)
            -- Standard format: 1234.56 or $1,234.56
            ELSE
                CAST(REGEXP_REPLACE(amount, '[\$\€£\s]', '') AS DOUBLE PRECISION)
        END AS "Amount",
        TRIM(UPPER(currencyisocode)) AS "CurrencyIsoCode",
        CAST(accountid AS TEXT) AS "AccountId",
        id AS "Legacy_Opportunity_ID__c",
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM source_data
)

SELECT * FROM transformed
