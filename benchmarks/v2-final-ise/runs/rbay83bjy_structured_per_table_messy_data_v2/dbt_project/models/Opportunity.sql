{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    src.name AS "Name",
    CASE
        WHEN UPPER(TRIM(src.stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(src.stagename)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(src.stagename)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(src.stagename)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(src.stagename)) IN ('ID. DECISION MAKERS', 'PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(src.stagename)) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(src.stagename)) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(src.stagename)) IN ('CLOSED WON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(src.stagename)) IN ('CLOSED LOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(TRIM(src.stagename)) IN ('IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(src.stagename)) IN ('IN PRÜFUNG') THEN 'Qualification'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN src.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(src.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN src.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN src.amount ~ '^[+-]?\d+\.\d{3},\d{2}$' THEN 
            CAST(REPLACE(REPLACE(src.amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[+-]?\d+,\d{2}$' THEN 
            CAST(REPLACE(src.amount, ',', '.') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[+-]?\d+\.\d{2}$' THEN 
            CAST(src.amount AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[A-Z]{2,3} [+-]?\d+\.\d{2}$' THEN 
            CAST(REGEXP_REPLACE(src.amount, '^[A-Z]{2,3} ', '', 'g') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[+-]?\d+$' THEN 
            CAST(src.amount AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('CHF', 'SWISS FRANC', '£') THEN 'CHF'
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('GBP') THEN 'GBP'
        ELSE src.currencyisocode
    END AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src