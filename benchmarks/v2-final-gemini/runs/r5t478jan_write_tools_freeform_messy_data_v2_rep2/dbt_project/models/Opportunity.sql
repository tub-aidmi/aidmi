{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    CASE
        WHEN TRIM(INITCAP(stagename)) = 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(INITCAP(stagename)) = 'Qualification' THEN 'Qualification'
        WHEN TRIM(INITCAP(stagename)) = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(INITCAP(stagename)) = 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(INITCAP(stagename)) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(INITCAP(stagename)) = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(INITCAP(stagename)) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(INITCAP(stagename)) = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(INITCAP(stagename)) IN ('Closed Won', 'Won') THEN 'Closed Won'
        WHEN TRIM(INITCAP(stagename)) IN ('Closed Lost', 'Lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    CASE
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount ~ '^\s*[€$£]?\s*\d{1,3}(\.\d{3})*,\d{2}\s*$' THEN -- European format (1.234,56)
            REPLACE(REPLACE(REGEXP_REPLACE(amount, '^\s*[€$£]?\s*|', ''), '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN amount ~ '^\s*[€$£]?\s*\d{1,3}(,\d{3})*\.\d{2}\s*$' THEN -- US/Standard format (1,234.56)
            REPLACE(REGEXP_REPLACE(amount, '^\s*[€$£]?\s*|', ''), ',', '')::DOUBLE PRECISION
        WHEN amount ~ '^\s*[€$£]?\s*\d+\s*$' THEN -- Whole number
            REGEXP_REPLACE(amount, '^\s*[€$£]?\s*|', '')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
