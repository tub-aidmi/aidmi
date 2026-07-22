{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    o.name AS "Name",
    CASE 
        WHEN TRIM(LOWER(o.stagename)) IN ('prospecting', 'qualification', 'needs analysis', 'value proposition', 'id. decision makers', 'perception analysis', 'proposal/price quote', 'negotiation/review', 'closed won', 'closed lost') 
        THEN INITCAP(REGEXP_REPLACE(TRIM(o.stagename), 'Id\. Decision Makers', 'Id. Decision Makers', 'g'))
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
        WHEN o.amount ~ '^[0-9]+\.?[0-9]*$' THEN o.amount::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+,[0-9]+$' THEN REPLACE(o.amount, ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(o.amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN o.amount ~ '^[€$£][0-9]+\.?[0-9]*$' THEN REGEXP_REPLACE(o.amount, '[^0-9.]', '', 'g')::DOUBLE PRECISION
        ELSE NULL 
    END AS "Amount",
    o.currencyisocode AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o