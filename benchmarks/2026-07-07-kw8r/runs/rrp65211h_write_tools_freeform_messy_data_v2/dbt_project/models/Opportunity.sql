{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    TRIM(name) AS "Name",
    CASE 
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING') THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) IN ('QUALIFICATION') THEN 'Qualification'
        WHEN UPPER(TRIM(stagename)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED LOST', 'LOST', 'IN PRÜFUNG') THEN 'Closed Lost'
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECT') THEN 'Prospecting'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN closedate ~ '^[0-9]{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN closedate
        WHEN closedate IS NULL THEN NULL
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(amount, '\.', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+,[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(amount, ',', '.') AS DOUBLE PRECISION)
        WHEN amount ~ '^[A-Za-z]+ [0-9]+\.[0-9]+$' THEN 
            CAST(REGEXP_REPLACE(amount, '[A-Za-z]+ ', '') AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+\.[0-9]+$' THEN 
            CAST(amount AS DOUBLE PRECISION)
        WHEN amount ~ '^-[0-9]+\.[0-9]+$' THEN 
            CAST(amount AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+$' THEN 
            CAST(amount AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    TRIM(currencyisocode) AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
