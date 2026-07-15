{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Unnamed Opportunity') AS "Name",
    CASE 
        WHEN TRIM(LOWER(o.stagename)) IN ('prospecting', 'qualification', 'needs analysis', 'value proposition', 'id. decision makers', 'perception analysis', 'proposal/price quote', 'negotiation/review', 'closed won', 'closed lost') 
        THEN INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(o.stagename), 'Id\.', 'Id '), 'Price Quote', 'Price Quote'))
        WHEN TRIM(LOWER(o.stagename)) IN ('prospecting', 'qualification', 'needs analysis', 'value proposition', 'id decision makers', 'perception analysis', 'proposal', 'negotiation', 'won', 'lost') 
        THEN INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(o.stagename), 'Id ', 'Id. '), 'Price Quote', 'Price Quote'))
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN TRIM(o.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(o.closedate)
        WHEN TRIM(o.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(o.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(o.closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(o.closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN TRIM(o.amount) ~ '^[0-9]+\.[0-9]{2}$' THEN CAST(TRIM(o.amount) AS DOUBLE PRECISION)
        WHEN TRIM(o.amount) ~ '^[0-9]+,[0-9]{2}$' THEN CAST(REPLACE(TRIM(o.amount), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(o.amount) ~ '^[0-9]+$' THEN CAST(TRIM(o.amount) AS DOUBLE PRECISION)
        WHEN TRIM(o.amount) ~ '^[0-9]+\.[0-9]+\.[0-9]+$' THEN CAST(REPLACE(REPLACE(TRIM(o.amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(o.amount) ~ '^[0-9.]+$' THEN CAST(REGEXP_REPLACE(TRIM(o.amount), '[^0-9]', '', 'g') AS DOUBLE PRECISION) / 100.0
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN TRIM(LOWER(o.currencyisocode)) IN ('usd', 'eur', 'gbp', 'jpy', 'cad', 'aud') THEN UPPER(TRIM(o.currencyisocode))
        ELSE NULL
    END AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"