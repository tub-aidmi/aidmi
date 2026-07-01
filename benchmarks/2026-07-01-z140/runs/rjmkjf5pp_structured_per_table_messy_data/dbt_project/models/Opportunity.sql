{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(TRIM("Name"), '') AS "Name",
    CASE
        WHEN LOWER(TRIM(COALESCE("StageName", ''))) IN ('prospect', 'prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(COALESCE("StageName", ''))) LIKE 'quali%' THEN 'Qualification'
        WHEN LOWER(TRIM(COALESCE("StageName", ''))) IN (
            'closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)'
        ) THEN 'Closed Won'
        WHEN LOWER(TRIM(COALESCE("StageName", ''))) IN (
            'closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)'
        ) THEN 'Closed Lost'
        WHEN LOWER(TRIM(COALESCE("StageName", ''))) = 'in kontakt' THEN 'Prospecting'
        WHEN LOWER(TRIM(COALESCE("StageName", ''))) = 'in prüfung' THEN 'Needs Analysis'
        ELSE 'Needs Analysis'
    END AS "StageName",
    CASE
        WHEN TRIM(COALESCE("CloseDate", '')) = '' OR TRIM(COALESCE("CloseDate", '')) = 'N/A' THEN NULL
        WHEN TRIM(COALESCE("CloseDate", '')) = '0000-00-00' THEN NULL
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(TRIM("CloseDate") AS TEXT)
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            TO_CHAR(TO_DATE(TRIM("CloseDate"), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{8}$' THEN 
            TO_CHAR(TO_DATE(TRIM("CloseDate"), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(COALESCE("CloseDate", '')) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(TRIM("CloseDate"), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN UPPER(TRIM(COALESCE("Amount", ''))) = 'NONE' THEN NULL
        ELSE CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        TRIM(COALESCE("Amount", '')),
                        '[^\d.,\-]', '', 'g'
                    ),
                    '\.', '', 'g'
                ),
              ',', '.')::DOUBLE PRECISION
        AS DOUBLE PRECISION)
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(COALESCE("CurrencyIsoCode", ''))) = 'DOLLAR' THEN 'USD'
        WHEN LOWER(TRIM(COALESCE("CurrencyIsoCode", ''))) IN ('€', 'eur') THEN 'EUR'
        WHEN UPPER(TRIM(COALESCE("CurrencyIsoCode", ''))) IN ('CHF', 'GBP', 'USD', 'EUR') THEN 
            UPPER(TRIM("CurrencyIsoCode"))
        ELSE NULL
    END AS "CurrencyIsoCode",
    "AccountId" AS "AccountId",
    "Id" AS "Legacy_Opportunity_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Opportunity') }}