{{ config(materialized='table') }}
SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stagename)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stagename)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stagename)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stagename)) IN ('id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stagename)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.stagename)) IN ('in prüfung', 'in kontakt') THEN NULL
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN LOWER(TRIM(o.amount)) = 'none' THEN NULL
        WHEN o.amount ~ '^[+-]?\d+(\.\d{3})*,\d{2}$' THEN CAST(REPLACE(REGEXP_REPLACE(o.amount, '\.', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[+-]?\d+,\d{2}$' THEN CAST(REPLACE(o.amount, ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[+-]?\d+\.\d+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[+-]?\d+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '[A-Za-z]' THEN CAST(REGEXP_REPLACE(o.amount, '[^0-9.-]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('euro', 'eur') THEN 'EUR'
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('dollar', 'usd', '$') THEN 'USD'
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('chf', 'swiss franc') THEN 'CHF'
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('£', 'gbp', 'pound') THEN 'GBP'
        ELSE o.currencyisocode
    END AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o