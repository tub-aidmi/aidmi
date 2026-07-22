{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(id)) AS "Id",
    INITCAP(TRIM(COALESCE(name, 'Unknown'))) AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN TRIM(closedate) ~ '^\d{4}[-/]\d{2}[-/]\d{2}$' THEN TO_DATE(TRIM(closedate), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(closedate), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_DATE(TRIM(closedate), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        -- European with thousands separator (e.g., 1.234,56): remove dots, swap comma to period
        WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(amount, '\$|EUR\s*', '', 'gi')), '[^0-9.,]', '') ~ '\.\d{3},\d+$' THEN
            REPLACE(REPLACE(TRIM(REGEXP_REPLACE(amount, '\$|EUR\s*', '', 'gi')), '.', ''), ',', '.')::DOUBLE PRECISION
        -- European without thousands (e.g., 1234,56): swap comma to period
        WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(amount, '\$|EUR\s*', '', 'gi')), '[^0-9.,]', '') ~ ',\d{2}$' AND TRIM(REGEXP_REPLACE(amount, '\$|EUR\s*', '', 'gi')) !~ '\.' THEN
            REPLACE(TRIM(REGEXP_REPLACE(amount, '\$|EUR\s*', '', 'gi')), ',', '.')::DOUBLE PRECISION
        -- Standard format (e.g., 1.234.56 or $1,234.56): remove non-numeric except digits/dot/commas, strip thousand commas
        ELSE REGEXP_REPLACE(TRIM(REGEXP_REPLACE(amount, '[^0-9.,]', '')), ',(?=\d{3})', '')::DOUBLE PRECISION
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    TRIM(UPPER(accountid)) AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}