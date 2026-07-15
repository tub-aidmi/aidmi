{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN TRIM(LOWER(stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER(stagename)) IN ('qualification') THEN 'Qualification'
        WHEN TRIM(LOWER(stagename)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(stagename)) IN ('value proposition', 'value_prop') THEN 'Value Proposition'
        WHEN TRIM(LOWER(stagename)) IN ('id. decision makers', 'identify decision makers', 'id_decision_makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(stagename)) IN ('closed won', 'closed_won') THEN 'Closed Won'
        WHEN TRIM(LOWER(stagename)) IN ('closed lost', 'closed_lost') THEN 'Closed Lost'
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
        WHEN amount ~ '^\$[0-9]+\.[0-9]+$' THEN REPLACE(amount, '$', '')::DOUBLE PRECISION
        WHEN amount ~ '^\$[0-9]+$' THEN REPLACE(amount, '$', '')::DOUBLE PRECISION
        WHEN amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN amount ~ '^[0-9]+$' THEN amount::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(UPPER(currencyisocode)) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
