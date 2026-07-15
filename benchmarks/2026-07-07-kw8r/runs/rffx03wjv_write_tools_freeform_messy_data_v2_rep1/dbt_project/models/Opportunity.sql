{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING', 'PROSPECT', 'IN KONTAKT') THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(stagename)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) IN ('NEGOTIATION/REVIEW', 'NEGOTIATION') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED WON', 'CLOSEDWON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED LOST', 'CLOSEDLOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)', 'IN PRÜFUNG') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN closedate = '0000-00-00' THEN NULL
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' OR UPPER(TRIM(amount)) = 'NONE' THEN NULL
        WHEN amount ~ '^[0-9]+\.[0-9]{3},[0-9]{2}$' THEN CAST(REPLACE(REPLACE(amount, '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN amount ~ '^[0-9]+,[0-9]{2}$' THEN CAST(REPLACE(amount, ',', '.') AS DOUBLE PRECISION)
        WHEN amount ~ '^[A-Z]{2,4} [0-9]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE(amount, '^[A-Z]{2,4} ', '') AS DOUBLE PRECISION)
        WHEN amount ~ '^[A-Z]{2,4}[0-9]+\.[0-9]+$' THEN CAST(REGEXP_REPLACE(amount, '^[A-Z]{2,4}', '') AS DOUBLE PRECISION)
        WHEN amount ~ '^-?[0-9]+\.[0-9]+$' THEN CAST(amount AS DOUBLE PRECISION)
        WHEN amount ~ '^-?[0-9]+$' THEN CAST(amount AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
