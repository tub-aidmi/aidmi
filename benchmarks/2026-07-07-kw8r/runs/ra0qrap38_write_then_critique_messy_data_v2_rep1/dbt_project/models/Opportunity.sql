{{ config(materialized='table') }}
SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Untitled Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED WON', 'WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED LOST', 'LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(stagename)) IN ('QUALIFICATION', 'QUALIFIKATION', 'QUALI') THEN 'Qualification'
        WHEN UPPER(TRIM(stagename)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(stagename)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(stagename)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(stagename)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(stagename)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(stagename)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        ELSE NULL
    END AS "StageName",
    COALESCE(
        NULLIF(
            CASE
                WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(closedate)
                WHEN TRIM(closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
                WHEN TRIM(closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
                WHEN TRIM(closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
                WHEN TRIM(closedate) ~ '^\d{1,2}-\d{1,2}-\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(closedate), 'DD-MM-YYYY'), 'YYYY-MM-DD')
                ELSE NULL
            END, ''
        ), NULL
    ) AS "CloseDate",
    CASE
        WHEN TRIM(amount) ~ '^[\d.,-]+$' THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '\.', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(currencyisocode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(currencyisocode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(currencyisocode)) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM(currencyisocode)) IN ('GBP', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}