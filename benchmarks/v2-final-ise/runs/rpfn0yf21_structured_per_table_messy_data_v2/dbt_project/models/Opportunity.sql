{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    -- Stage mapping: normalize all source variants to the 10 allowed enum values
    CASE
        WHEN LOWER(TRIM(o.stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(o.stagename)) IN ('prospecting', 'prospect', 'prospektion') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stagename)) IN ('qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stagename)) = 'in prüfung' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stagename)) = 'in kontakt' THEN 'Qualification'
        ELSE NULL
    END AS "StageName",
    -- CloseDate: handle YYYYMMDD, MM/DD/YYYY, DD.MM.YYYY, and ISO formats
    CASE
        WHEN o.closedate IS NULL THEN NULL
        WHEN o.closedate ~ '^\d{8}$' THEN
            TO_DATE(o.closedate, 'YYYYMMDD')::TEXT
        WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_DATE(o.closedate, 'MM/DD/YYYY')::TEXT
        WHEN o.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_DATE(o.closedate, 'DD.MM.YYYY')::TEXT
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN
            CAST(o.closedate AS DATE)::TEXT
        ELSE NULL
    END AS "CloseDate",
    -- Amount: strip currency text/prefixes, handle European format (X.XXX,XX), guard against empty strings
    CASE
        WHEN TRIM(o.amount) IS NULL OR TRIM(o.amount) = '' THEN NULL
        ELSE CAST(
            NULLIF(REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        LOWER(TRIM(o.amount)),
                         '(usd|eur|gbp|chf|€|£|$|%|\d*\s)', '', 'gi'
                     ),
                     '(\d+)\.(\d{3}),(\d+)', '\1\2.\3', 'g'
                 ),
                 '[^\d.-]', '', 'g'
             ), '') AS DOUBLE PRECISION)
    END AS "Amount",
    -- CurrencyIsoCode: normalize symbols/names to ISO codes, fix standalone 'Dollar' mapping
    CASE
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('usd', 'us dollar', 'us dollars', 'dollar', 'dollars', '$') THEN 'USD'
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('eur', 'euro', 'euros', '€') THEN 'EUR'
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('gbp', 'british pound', '£') THEN 'GBP'
        WHEN LOWER(TRIM(o.currencyisocode)) IN ('chf', 'swiss franc') THEN 'CHF'
        WHEN o.currencyisocode IS NULL OR TRIM(o.currencyisocode) = '' THEN NULL
        ELSE UPPER(TRIM(o.currencyisocode))
    END AS "CurrencyIsoCode",
    -- AccountId: source CUST-XXXX maps directly to Salesforce-style Account.Id (CUST-XXXX)
    o.accountid AS "AccountId",
    -- Legacy key for row-level verification
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o