{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN LOWER(TRIM(o.stagename)) IN ('prospecting', 'qualification', 'needs analysis', 'value proposition', 'id. decision makers', 'perception analysis', 'proposal/price quote', 'negotiation/review', 'closed won', 'closed lost') 
        THEN INITCAP(REGEXP_REPLACE(LOWER(TRIM(o.stagename)), 'id\. decision makers', 'Id. Decision Makers'))
        WHEN LOWER(TRIM(o.stagename)) = 'proposal' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stagename)) = 'negotiation' THEN 'Negotiation/Review'
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
        WHEN o.amount ~ '^[0-9]+([,.][0-9]+)?$' THEN 
            CASE 
                WHEN o.amount ~ ',' AND o.amount ~ '\.' THEN 
                    CAST(REPLACE(REPLACE(o.amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
                WHEN o.amount ~ ',' THEN 
                    CAST(REPLACE(o.amount, ',', '.') AS DOUBLE PRECISION)
                ELSE 
                    CAST(o.amount AS DOUBLE PRECISION)
            END
        WHEN o.amount ~ '^[^0-9]+([0-9]+([,.][0-9]+)?)$' THEN 
            CASE 
                WHEN REGEXP_REPLACE(o.amount, '[^0-9.,]', '') ~ ',' AND REGEXP_REPLACE(o.amount, '[^0-9.,]', '') ~ '\.' THEN 
                    CAST(REPLACE(REPLACE(REGEXP_REPLACE(o.amount, '[^0-9.,]', ''), '.', ''), ',', '.') AS DOUBLE PRECISION)
                WHEN REGEXP_REPLACE(o.amount, '[^0-9.,]', '') ~ ',' THEN 
                    CAST(REPLACE(REGEXP_REPLACE(o.amount, '[^0-9.,]', ''), ',', '.') AS DOUBLE PRECISION)
                ELSE 
                    CAST(REGEXP_REPLACE(o.amount, '[^0-9.,]', '') AS DOUBLE PRECISION)
            END
        ELSE NULL 
    END AS "Amount",
    COALESCE(NULLIF(TRIM(o.currencyisocode), ''), 'USD') AS "CurrencyIsoCode",
    o(accountid) AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o