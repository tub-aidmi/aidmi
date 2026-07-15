{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Unnamed Opportunity') AS "Name",
    CASE 
        WHEN TRIM(LOWER(o.stagename)) IN ('prospecting', 'qualification', 'needs analysis', 'value proposition', 'id. decision makers', 'perception analysis', 'proposal/price quote', 'negotiation/review', 'closed won', 'closed lost') 
        THEN INITCAP(REGEXP_REPLACE(TRIM(LOWER(o.stagename)), 'id\. decision makers', 'Id. Decision Makers', 'g'))
        WHEN TRIM(LOWER(o.stagename)) IN ('proposal', 'price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.stagename)) IN ('negotiation', 'review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.stagename)) = 'won' THEN 'Closed Won'
        WHEN TRIM(LOWER(o.stagename)) = 'lost' THEN 'Closed Lost'
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
        WHEN o.amount ~ '^[0-9]+\.[0-9]{2}$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[0-9]+,[0-9]{2}$' THEN CAST(REPLACE(o.amount, ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN CAST(REPLACE(REPLACE(o.amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[0-9]+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '^\D*([0-9]+\.?[0-9]*)' THEN CAST(REGEXP_REPLACE(o.amount, '[^0-9.]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    NULLIF(TRIM(o.currencyisocode), '') AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON o.accountid = a.id