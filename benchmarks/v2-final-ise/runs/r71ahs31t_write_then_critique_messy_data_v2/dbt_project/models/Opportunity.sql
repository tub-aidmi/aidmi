{{ config(materialized='table') }}
SELECT
    src.id AS "Id",
    src.name AS "Name",
    CASE
        WHEN UPPER(TRIM(src.stagename)) IN ('CLOSED WON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(src.stagename)) IN ('CLOSED LOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(TRIM(src.stagename)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(src.stagename)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(src.stagename)) IN ('IN PRÜFUNG') THEN 'Needs Analysis'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN src.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(src.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN src.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN src.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(src.amount, '\.', '', 'g'), ',', '.', 'g') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[0-9]+,[0-9]+$' THEN CAST(REGEXP_REPLACE(src.amount, ',', '.', 'g') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[A-Za-z]+ [0-9]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE(src.amount, '[^0-9\.-]', '', 'g') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[A-Za-z]+[0-9]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE(src.amount, '[^0-9\.-]', '', 'g') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[0-9\-]+\.[0-9]+$' THEN src.amount::DOUBLE PRECISION
        WHEN src.amount ~ '^[0-9\-]+$' THEN src.amount::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('CHF', 'SWISS FRANC') THEN 'CHF'
        WHEN UPPER(TRIM(src.currencyisocode)) IN ('GBP', '£') THEN 'GBP'
        ELSE UPPER(TRIM(src.currencyisocode))
    END AS "CurrencyIsoCode",
    src.accountid AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} src