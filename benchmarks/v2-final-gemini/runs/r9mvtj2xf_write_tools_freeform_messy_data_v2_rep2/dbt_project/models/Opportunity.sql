{{ config(materialized='table') }}

WITH cleaned_amounts_prep AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        currencyisocode,
        accountid,
        TRIM(REGEXP_REPLACE(LOWER(amount), 'eur\s*', '', 'g')) AS amount_stripped_currency -- Strip 'eur ' prefix
    FROM
        {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
cleaned_amounts_normalized AS (
    SELECT
        *,
        CASE
            WHEN amount_stripped_currency IS NULL OR amount_stripped_currency = '' THEN NULL
            WHEN amount_stripped_currency LIKE '%,%' AND amount_stripped_currency LIKE '%.%' THEN
                -- Contains both '.' and ','
                CASE
                    -- European format: dot is thousand, comma is decimal (e.g., 1.234,56)
                    WHEN POSITION('.' IN amount_stripped_currency) < POSITION(',' IN amount_stripped_currency) THEN
                        REPLACE(REPLACE(amount_stripped_currency, '.', ''), ',', '.')
                    -- American format: comma is thousand, dot is decimal (e.g., 1,234.56)
                    WHEN POSITION(',' IN amount_stripped_currency) < POSITION('.' IN amount_stripped_currency) THEN
                        REPLACE(amount_stripped_currency, ',', '')
                    ELSE NULL -- ambiguous or invalid mixed separators, or other unexpected format
                END
            WHEN amount_stripped_currency LIKE '%,%' THEN -- Only comma, assume decimal
                REPLACE(amount_stripped_currency, ',', '.')
            WHEN amount_stripped_currency LIKE '%.%' THEN -- Only dot, assume decimal
                amount_stripped_currency
            WHEN amount_stripped_currency ~ '^-?\d+$' THEN -- Integer
                amount_stripped_currency
            ELSE NULL -- Unparseable format
        END AS amount_cleaned_decimal
    FROM
        cleaned_amounts_prep
)
SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CASE
        WHEN LOWER(stagename) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(stagename) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(stagename) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(stagename) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(stagename) = 'in prüfung' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        TO_CHAR(CASE
            WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')
            WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')
            WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')
            ELSE NULL
        END, 'YYYY-MM-DD'),
        '1900-01-01' -- Default for NOT NULL target
    ) AS "CloseDate",
    CASE
        WHEN amount_cleaned_decimal ~ '^-?\d+(\.\d+)?$' THEN amount_cleaned_decimal::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c", -- Using source id as legacy ID
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_amounts_normalized
