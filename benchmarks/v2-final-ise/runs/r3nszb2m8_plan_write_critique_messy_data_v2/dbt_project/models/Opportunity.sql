{{ config(materialized='table') }}

SELECT
    TRIM(o."id") AS "Id",
    INITCAP(TRIM(o."name")) AS "Name",
    CASE
        WHEN LOWER(TRIM(o."stagename")) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(o."stagename")) IN ('qualification', 'quali', 'qualifikation', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(TRIM(o."stagename")) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o."stagename")) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o."stagename")) IN ('id. decision makers', 'identifizierung entscheidungsträger') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o."stagename")) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o."stagename")) IN ('proposal/price quote', 'angebot/preisanfrage') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o."stagename")) IN ('negotiation/review', 'verhandlung/prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o."stagename")) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o."stagename")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(o."closedate") ~ '^\d{8}$' THEN TO_DATE(TRIM(o."closedate"), 'YYYYMMDD')::TEXT
        WHEN TRIM(o."closedate") ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(o."closedate")::DATE::TEXT
        WHEN TRIM(o."closedate") ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(o."closedate"), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(o."closedate") ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(o."closedate"), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(o."amount") IS NULL OR LOWER(TRIM(o."amount")) IN ('none', 'null', '') THEN NULL
        WHEN REGEXP_REPLACE(TRIM(o."amount"), '^[A-Za-z€$£]+\s*', '') ~ '\.\d{1,3},\d+$' THEN
            CAST(
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(TRIM(o."amount"), '^([A-Za-z€$£]\s*)', ''),
                        '.', ''
                    ),
                    ',', '.'
                ) AS DOUBLE PRECISION)
        ELSE
            CAST(
                REGEXP_REPLACE(TRIM(o."amount"), '^[A-Za-z€$£]+(\s+)?', '')
            AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(o."currencyisocode")) IN ('USD', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(o."currencyisocode")) IN ('EUR', 'EURO') OR TRIM(o."currencyisocode") = '€' THEN 'EUR'
        WHEN UPPER(TRIM(o."currencyisocode")) IN ('CHF', 'CHF') THEN 'CHF'
        WHEN UPPER(TRIM(o."currencyisocode")) IN ('GBP', '£') THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    TRIM(a."id") AS "AccountId",
    TRIM(o."id") AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON TRIM(o."accountid") = TRIM(a."id")