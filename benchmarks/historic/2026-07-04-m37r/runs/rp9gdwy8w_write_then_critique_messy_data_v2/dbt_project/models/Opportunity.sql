{{ config(materialized='table') }}

WITH cleaned_amounts AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        -- Remove any characters that are not digits, minus sign, dot, or comma
        REGEXP_REPLACE(TRIM(amount), '[^0-9\.\,\-]', '', 'g') AS amount_raw_cleaned,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
final_parsed_amounts AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        currencyisocode,
        accountid,
        -- Normalize amount based on identified format.
        -- Prioritize specific formats (European/US with thousand separators) before general ones.
        CASE
            -- European format with dot as thousand separator and comma as decimal (e.g., 1.234,56)
            WHEN amount_raw_cleaned ~ '^-?\d{1,3}(?:\.\d{3})*,\d+$' THEN
                REPLACE(REPLACE(amount_raw_cleaned, '.', ''), ',', '.')
            -- US format with comma as thousand separator and dot as decimal (e.g., 1,234.56)
            WHEN amount_raw_cleaned ~ '^-?\d{1,3}(?:,\d{3})*\.\d+$' THEN
                REPLACE(amount_raw_cleaned, ',', '')
            -- European-style decimal without thousand separators (e.g., 1234,56)
            WHEN amount_raw_cleaned ~ '^-?\d+,\d+$' THEN
                REPLACE(amount_raw_cleaned, ',', '.')
            -- Standard decimal or integer format (e.g., 1234.56 or 1234)
            WHEN amount_raw_cleaned ~ '^-?\d+(?:\.\d+)?$' THEN
                amount_raw_cleaned
            ELSE NULL
        END AS amount_normalized_text
    FROM cleaned_amounts
)
SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('won', 'closed won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('lost', 'closed lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('in prüfung', 'in kontakt') THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL target column, consistent with enum
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD') -- YYYY-MM-DD
                WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD') -- YYYYMMDD
                WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY') -- DD.MM.YYYY
                WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY') -- M/D/YYYY or MM/DD/YYYY
                ELSE NULL
            END, 'YYYY-MM-DD'
        ), '1900-01-01' -- Default for NOT NULL target column when unparseable, as NULL is not allowed
    ) AS "CloseDate",
    -- Explicitly CAST the normalized text to DOUBLE PRECISION
    CASE
        WHEN amount_normalized_text IS NOT NULL AND amount_normalized_text ~ '^-?\d+(?:\.\d+)?$' THEN
            CAST(amount_normalized_text AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN UPPER(TRIM(currencyisocode)) IN ('EURO', 'EUR', '€') THEN 'EUR'
        WHEN UPPER(TRIM(currencyisocode)) IN ('DOLLAR', 'USD', '$') THEN 'USD'
        WHEN UPPER(TRIM(currencyisocode)) IN ('GBP', '£') THEN 'GBP'
        WHEN UPPER(TRIM(currencyisocode)) = 'CHF' THEN 'CHF'
        ELSE NULL
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM final_parsed_amounts