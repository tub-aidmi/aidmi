{{ config(materialized='table') }}
SELECT
    src.id AS "Id",
    src.name AS "Name",
    CASE
        WHEN LOWER(TRIM(src.stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(src.stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(src.stagename)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(src.stagename)) IN ('qualification', 'quali', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(src.stagename)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(src.stagename)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(src.stagename)) IN ('id. decision makers', 'decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(src.stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(src.stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(src.stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN src.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN src.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN src.amount ~ '^[0-9]+\.[0-9]+,[0-9]+$' THEN CAST(REPLACE(REPLACE(src.amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[0-9\-]+\.[0-9]+$' THEN CAST(src.amount AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[0-9\-]+$' THEN CAST(src.amount AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[A-Za-z\s]+[0-9]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE(src.amount, '[^0-9.-]', '', 'g') AS DOUBLE PRECISION)
        WHEN src.amount ~ '^[A-Za-z\s]+[0-9]+$' THEN CAST(REGEXP_REPLACE(src.amount, '[^0-9-]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(src.currencyisocode)) IN ('euro', 'eur') THEN 'EUR'
        WHEN LOWER(TRIM(src.currencyisocode)) IN ('dollar', 'usd', '$') THEN 'USD'
        WHEN LOWER(TRIM(src.currencyisocode)) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM(src.currencyisocode)) IN ('£', 'gbp') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    a.id AS "AccountId",
    src.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} AS a ON src.accountid = a.id