{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'quali', 'qualification', 'qualifikation') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(stagename)) IN ('in kontakt', 'in prüfung') THEN 'Qualification'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount IS NULL THEN NULL
        WHEN amount ~ '^[0-9\-]+\.?[0-9]*$' THEN amount::DOUBLE PRECISION
        WHEN amount ~ '^[0-9\-]+,[0-9]+$' THEN 
            REPLACE(REPLACE(amount, '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN amount ~ '^[^0-9\-]+([0-9\-]+\.?[0-9]*)' THEN 
            REGEXP_REPLACE(amount, '[^0-9\-.]', '', 'g')::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(currencyisocode)) IN ('usd', '$') THEN 'USD'
        WHEN LOWER(TRIM(currencyisocode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(currencyisocode)) IN ('chf') THEN 'CHF'
        WHEN LOWER(TRIM(currencyisocode)) IN ('gbp', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }}
