{{ config(materialized='table') }}

SELECT 
    CAST(id AS TEXT) AS "Id",
    TRIM(name) AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('prospect', 'prospecting', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'quali', 'qualifikation', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'id decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)', 'won') THEN 'Closed Won'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(closedate), 'YYYY-MM-DD')::TEXT
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(TRIM(closedate), 'YYYYMMDD')::TEXT
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(closedate), 'MM/DD/YYYY')::TEXT
        WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' OR UPPER(TRIM(amount)) = 'NONE' THEN NULL
        WHEN REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g') LIKE '%.%' 
             AND REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g') LIKE '%,%' THEN
            CAST(REGEXP_REPLACE(
                REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,-]', '', 'g'), '\.', ''), 
                ',', '.'
             ) AS DOUBLE PRECISION)
        ELSE
            CAST(REGEXP_REPLACE(amount, '[^0-9.,-]', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE 
        WHEN UPPER(TRIM(currencyisocode)) IN ('EUR', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(currencyisocode)) IN ('CHF') THEN 'CHF'
        WHEN UPPER(TRIM(currencyisocode)) IN ('USD', 'DOLLAR') THEN 'USD'
        WHEN TRIM(currencyisocode) = '£' THEN 'GBP'
        WHEN UPPER(TRIM(currencyisocode)) IN ('GBP', 'POUND') THEN 'GBP'
        ELSE UPPER(TRIM(currencyisocode))
    END AS "CurrencyIsoCode",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}