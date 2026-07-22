{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN UPPER(TRIM(o.stagename)) IN ('CLOSED WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)', 'WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stagename)) IN ('CLOSED LOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(TRIM(o.stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stagename)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(o.stagename)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stagename)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stagename)) IN ('ID. DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stagename)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stagename)) IN ('PROPOSAL/PRICE QUOTE') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stagename)) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN o.amount ~ '^[0-9.-]+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '[,]' THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(o.amount, '[.]', '', 'g'), ',', '.', 'g') AS DOUBLE PRECISION)
        ELSE CAST(REGEXP_REPLACE(o.amount, '[^0-9.-]', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('EUR', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('USD', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('£', 'GBP') THEN 'GBP'
        ELSE UPPER(TRIM(o.currencyisocode))
    END AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o