{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualify') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value_prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'id decision makers', 'decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN amount ~ '^[0-9]+\.[0-9]+$' THEN amount::DOUBLE PRECISION
        WHEN amount ~ '^[0-9]+,[0-9]+$' THEN REPLACE(amount, ',', '.')::DOUBLE PRECISION
        WHEN amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN amount ~ '^[0-9]+$' THEN amount::DOUBLE PRECISION
        WHEN amount ~ '^\$[0-9]+\.[0-9]+$' THEN REPLACE(amount, '$', '')::DOUBLE PRECISION
        WHEN amount ~ '^€[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(amount, '€', ''), ',', '.')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(currencyisocode), '') AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}