{{ config(materialized='table') }}

SELECT 
    src.id AS "Id",
    INITCAP(TRIM(src.name)) AS "Name",
    CASE 
        WHEN INITCAP(TRIM(src.stagename)) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost') 
        THEN INITCAP(TRIM(src.stagename))
        ELSE NULL 
    END AS "StageName",
    CASE 
        WHEN src.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(src.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "CloseDate",
    CASE 
        WHEN src.amount ~ '^[\d]+[.,\d]+$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(src.amount, '[^\d.,]', ''), '(\d+)\.(\d+),(\d+)', '\1\2.\3') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[\d.,]+$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(src.amount, '\.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL 
    END AS "Amount",
    src.currencyisocode AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src