{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(o.stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stagename)) IN ('qualification', 'qualify') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stagename)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stagename)) IN ('value proposition', 'value_prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stagename)) IN ('id. decision makers', 'id_decision_makers', 'decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stagename)) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stagename)) IN ('proposal/price quote', 'proposal', 'price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stagename)) IN ('negotiation/review', 'negotiation', 'review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o.amount ~ '^[0-9]+\.[0-9]+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[0-9]+,[0-9]+$' THEN CAST(REPLACE(o.amount, ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN CAST(REPLACE(REPLACE(o.amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[0-9]+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[€$£]\s*[0-9]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE(o.amount, '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[€$£]\s*[0-9]+,[0-9]+$' THEN CAST(REGEXP_REPLACE(REPLACE(o.amount, ',', '.'), '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(o.currencyisocode), '') AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o