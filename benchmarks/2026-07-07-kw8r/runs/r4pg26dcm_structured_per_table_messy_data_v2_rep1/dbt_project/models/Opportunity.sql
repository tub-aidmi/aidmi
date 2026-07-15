{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    CASE
        -- Prospecting stages (English and German)
        WHEN UPPER(TRIM(stagename)) IN ('PROSPECTING', 'PROSPECT') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('in kontakt') THEN 'Prospecting'
        -- Qualification stages (English and German)
        WHEN UPPER(TRIM(stagename)) IN ('QUALI', 'QUALIFICATION', 'QUALIFIKATION') THEN 'Qualification'
        -- Needs Analysis (German "In Prüfung")
        WHEN LOWER(TRIM(stagename)) IN ('in prüfung') THEN 'Needs Analysis'
        -- Closed Won stages (English and German)
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED WON', 'GEWONNEN', 'ABGESCHLOSSEN (GEWONNEN)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('won', 'closed won') THEN 'Closed Won'
        -- Closed Lost stages (English and German)
        WHEN UPPER(TRIM(stagename)) IN ('CLOSED LOST', 'VERLOREN', 'ABGESCHLOSSEN (VERLOREN)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(stagename)) IN ('lost', 'closed lost') THEN 'Closed Lost'
        -- Fallback for unmapped stages or values not in the target enum
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate IS NULL THEN NULL
        -- ISO format: YYYY-MM-DD
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN closedate
        -- Compact numeric: YYYYMMDD
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
        -- US format: M/D/YYYY or MM/DD/YYYY
        WHEN closedate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_DATE(closedate, 'M/D/YYYY')::TEXT
        -- European format: DD.MM.YYYY
        WHEN closedate ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE CAST(
            CASE
                -- European format with thousands dot and decimal comma: e.g., "60.702,05"
                WHEN REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g') ~ '\.[0-9]{3},[0-9]{1,2}$' THEN
                    CAST(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g'), '\.', ''),
                            ',', '.') AS DOUBLE PRECISION)
                -- Comma as decimal separator without thousands separator: e.g., "1234,56"
                WHEN REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g') ~ '[,][0-9]{1,2}$'
                     AND REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g') !~ '\.[0-9]{3}' THEN
                    CAST(
                        REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g'), ',', '.')
                        AS DOUBLE PRECISION)
                -- Standard format (digits with optional decimal point and minus): e.g., "325776.81", "-383632.13"
                ELSE CAST(REGEXP_REPLACE(amount, '[^\d.,\-]', '', 'g') AS DOUBLE PRECISION)
            END
        ) AS "Amount"
    END,
    CASE
        WHEN currencyisocode IS NULL OR TRIM(currencyisocode) = '' THEN NULL
        ELSE
            CASE
                WHEN LOWER(TRIM(currencyisocode)) IN ('usd', 'dollar', '$') THEN 'USD'
                WHEN LOWER(TRIM(currencyisocode)) IN ('eur', 'euro', '€') THEN 'EUR'
                WHEN UPPER(TRIM(currencyisocode)) IN ('CHF') THEN 'CHF'
                WHEN LOWER(TRIM(currencyisocode)) IN ('gbp', 'pound', '£') THEN 'GBP'
                -- Already a valid 3-letter ISO code
                WHEN UPPER(TRIM(currencyisocode)) ~ '^[A-Z]{3}$' THEN UPPER(TRIM(currencyisocode))
                ELSE NULL
            END
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}