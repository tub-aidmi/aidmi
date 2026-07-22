{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(TRIM(o.name), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN UPPER(TRIM(o.stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN UPPER(TRIM(o.stagename)) IN ('QUALIFICATION', 'QUALI', 'QUALIFIKATION') THEN 'Qualification'
        WHEN UPPER(TRIM(o.stagename)) IN ('NEEDS ANALYSIS') THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.stagename)) IN ('VALUE PROPOSITION') THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.stagename)) IN ('ID. DECISION MAKERS', 'ID DECISION MAKERS') THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.stagename)) IN ('PERCEPTION ANALYSIS') THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.stagename)) IN ('PROPOSAL/PRICE QUOTE', 'PROPOSAL') THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.stagename)) IN ('NEGOTIATION/REVIEW') THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.stagename)) IN ('CLOSED WON', 'ABGESCHLOSSEN (GEWONNEN)', 'GEWONNEN', 'WON') THEN 'Closed Won'
        WHEN UPPER(TRIM(o.stagename)) IN ('CLOSED LOST', 'ABGESCHLOSSEN (VERLOREN)', 'VERLOREN', 'LOST') THEN 'Closed Lost'
        WHEN UPPER(TRIM(o.stagename)) IN ('IN KONTAKT', 'IN PRÜFUNG') THEN 'Prospecting'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN o.closedate ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        WHEN o.closedate IS NULL THEN NULL
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
        WHEN UPPER(TRIM(o.amount)) = 'NONE' THEN NULL
        WHEN o.amount ~ ',' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(o.amount, '\.', '', 'g'), ',', '.') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[0-9.-]+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '[A-Za-z\s]' THEN 
            CASE 
                WHEN REGEXP_REPLACE(o.amount, '[^0-9.-]', '', 'g') = '' THEN NULL
                ELSE CAST(REGEXP_REPLACE(o.amount, '[^0-9.-]', '', 'g') AS DOUBLE PRECISION)
            END
        ELSE NULL
    END AS "Amount",
    CASE 
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('USD', 'DOLLAR', '$') THEN 'USD'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('EUR', 'EURO', '€') THEN 'EUR'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM(o.currencyisocode)) IN ('GBP', '£') THEN 'GBP'
        WHEN o.currencyisocode IS NULL THEN NULL
        ELSE UPPER(TRIM(o.currencyisocode))
    END AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON o.accountid = a.id
