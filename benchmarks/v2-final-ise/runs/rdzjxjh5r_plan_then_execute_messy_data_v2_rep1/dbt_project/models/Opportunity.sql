{{ config(materialized='table') }}

SELECT 
    opp.id AS "Id",
    COALESCE(TRIM(opp.name), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(opp.stagename)) IN ('prospecting', 'qualification', 'needs analysis', 'value proposition', 'id. decision makers', 'perception analysis', 'proposal/price quote', 'negotiation/review', 'closed won', 'closed lost') 
        THEN INITCAP(LOWER(TRIM(opp.stagename)))
        WHEN LOWER(TRIM(opp.stagename)) = 'in prüfung' THEN 'Qualification'
        WHEN LOWER(TRIM(opp.stagename)) = 'lost' THEN 'Closed Lost'
        WHEN LOWER(TRIM(opp.stagename)) = 'prospect' THEN 'Prospecting'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN opp.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opp.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN opp.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN opp.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN opp.amount ~ '^[0-9\-]+(\.[0-9]+)?$' THEN opp.amount::DOUBLE PRECISION
        WHEN opp.amount ~ '^[0-9]+,[0-9]+$' THEN REPLACE(opp.amount, ',', '.')::DOUBLE PRECISION
        WHEN opp.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN REPLACE(REPLACE(opp.amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN LOWER(TRIM(opp.amount)) LIKE 'eur %' THEN REPLACE(REPLACE(LOWER(TRIM(opp.amount)), 'eur ', ''), ',', '.')::DOUBLE PRECISION
        WHEN LOWER(TRIM(opp.amount)) LIKE '£ %' THEN REPLACE(LOWER(TRIM(opp.amount)), '£ ', '')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(opp.currencyisocode)) AS "CurrencyIsoCode",
    acc.id AS "AccountId",
    opp.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} opp
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acc ON opp.accountid = acc.id