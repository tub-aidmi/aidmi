{{ config(materialized='table') }}

SELECT
    "Id",
    COALESCE(TRIM("Name"), '') AS "Name",
    CASE
        WHEN LOWER(TRIM("StageName")) IN ('prospect', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM("StageName")) LIKE 'quali%' THEN 'Qualification'
        WHEN LOWER(TRIM("StageName")) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM("StageName")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM("StageName")) = 'in kontakt' THEN 'Prospecting'
        WHEN LOWER(TRIM("StageName")) = 'in prüfung' THEN 'Needs Analysis'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN "CloseDate" IS NULL THEN NULL
        WHEN "CloseDate" ~ '^\d{4}-\d{2}-\d{2}$' AND "CloseDate" != '0000-00-00' THEN "CloseDate"
        WHEN "CloseDate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("CloseDate", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "CloseDate" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("CloseDate", 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN "CloseDate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE("CloseDate", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN UPPER(TRIM(COALESCE("Amount", ''))) = 'NONE' THEN NULL
        WHEN TRIM("Amount") = '' THEN NULL
        ELSE CAST(
            REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        TRIM("Amount"),
                        '[^\d.,\-]', '', 'g'),
                    '\.', '', 'g'),
                ',', '.') AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(COALESCE("CurrencyIsoCode", ''))) = 'DOLLAR' THEN 'USD'
        WHEN LOWER(TRIM(COALESCE("CurrencyIsoCode", ''))) IN ('€', 'eur') THEN 'EUR'
        WHEN UPPER(TRIM("CurrencyIsoCode")) IN ('CHF', 'GBP', 'USD', 'EUR') THEN UPPER(TRIM("CurrencyIsoCode"))
        ELSE NULL
    END AS "CurrencyIsoCode",
    "AccountId",
    "Id" AS "Legacy_Opportunity_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Opportunity') }}