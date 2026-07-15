{{ config(materialized='table') }}

WITH cleaned_amounts AS (
    SELECT
        *,
        CASE
            WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
            ELSE
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(amount), E'^[\\$€£]+', '', 'g'),
                        E'^(?:Euro|Dollar|EUR|USD|GBP|CHF)\\s+', '', 'gi'
                    ),
                    E'\\s', ''
                )
        END AS raw_amount
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)
SELECT
    CAST(id AS TEXT) AS "Id",
    name AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'angebot/preisanfrage') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
        WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN raw_amount IS NULL OR TRIM(raw_amount) = '' THEN 0.0
        WHEN raw_amount = '0' THEN 0.0
        -- European format: digits.digits,digits (e.g. 60.702,05 or 150.721,39)
        WHEN raw_amount ~ '^\d{1,3}(\.\d{3})+,\d+$' THEN
            CAST(REGEXP_REPLACE(raw_amount, E'[.]', '', 'g') || '.' || SPLIT_PART(raw_amount, ',', -1) AS DOUBLE PRECISION)
        -- European format: simple digits,digits (e.g. 1234,56 — no thousands dots)
        WHEN raw_amount ~ '^\d+,\d+$' THEN
            CAST(SPLIT_PART(raw_amount, ',', 1) || '.' || SPLIT_PART(raw_amount, ',', -1) AS DOUBLE PRECISION)
        -- US format with thousand separators: digits,digits,digits (e.g. 1,234,567.89)
        WHEN raw_amount ~ '^\d{1,3}(,\d{3})+\.\d+$' THEN
            CAST(REGEXP_REPLACE(raw_amount, E'[,]', '', 'g') AS DOUBLE PRECISION)
        -- US format without thousand separators: plain digits.digits or negative
        WHEN raw_amount ~ '^-?\d+\.\d+$' OR raw_amount ~ '^-?\d+$' THEN
            CAST(raw_amount AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN currencyisocode IS NULL OR TRIM(currencyisocode) = '' THEN NULL
        WHEN UPPER(TRIM(currencyisocode)) IN ('USD', '$', 'DOLLAR') THEN 'USD'
        WHEN UPPER(TRIM(currencyisocode)) IN ('EUR', 'EURO') THEN 'EUR'
        WHEN UPPER(TRIM(currencyisocode)) IN ('GBP', '£') THEN 'GBP'
        WHEN UPPER(TRIM(currencyisocode)) IN ('CHF') THEN 'CHF'
        ELSE UPPER(TRIM(currencyisocode))
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM cleaned_amounts