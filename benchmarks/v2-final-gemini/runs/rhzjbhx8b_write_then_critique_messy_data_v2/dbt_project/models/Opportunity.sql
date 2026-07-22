-- models/Opportunity.sql
{{ config(materialized='table') }}

WITH normalize_whitespace_and_remove_symbols AS (
    SELECT
        src.id,
        src.name,
        src.stagename,
        src.closedate,
        src.currencyisocode,
        src.accountid,
        -- Clean and normalize amount string by removing currency symbols and extra spaces, and trim final result
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                LOWER(src.amount),
                                'eur ', ''
                            ),
                            '$', ''
                        ),
                        '£', ''
                    ),
                    '€', '' -- Add Euro symbol removal
                ),
                ' ', '' -- Remove all spaces after currency symbols
            )
        ) AS amount_cleaned_str_initial
    FROM
        {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS src
),

normalized_amount_for_casting AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        currencyisocode,
        accountid,
        -- Further normalize amount string for casting by handling separators
        CASE
            WHEN amount_cleaned_str_initial IS NULL OR TRIM(amount_cleaned_str_initial) = '' THEN NULL
            -- European format with both dot and comma (e.g., 1.234,56): remove thousand separator dots, then swap comma to dot
            WHEN POSITION(',', amount_cleaned_str_initial) > 0 AND
                 POSITION('.', amount_cleaned_str_initial) > 0 AND
                 POSITION('.', amount_cleaned_str_initial) < POSITION(',', amount_cleaned_str_initial)
            THEN REPLACE(REPLACE(amount_cleaned_str_initial, '.', ''), ',', '.')
            -- US format with both comma and dot (e.g., 1,234.56): remove thousand separator commas
            WHEN POSITION(',', amount_cleaned_str_initial) > 0 AND
                 POSITION('.', amount_cleaned_str_initial) > 0 AND
                 POSITION(',', amount_cleaned_str_initial) < POSITION('.', amount_cleaned_str_initial)
            THEN REPLACE(amount_cleaned_str_initial, ',', '')
            -- Only comma, assume European decimal (e.g., 123,45): swap comma to dot
            WHEN POSITION(',', amount_cleaned_str_initial) > 0 AND
                 POSITION('.', amount_cleaned_str_initial) = 0
            THEN REPLACE(amount_cleaned_str_initial, ',', '.')
            -- Only dot or no separators, assume US/standard (e.g., 123.45 or 12345): use as is
            WHEN POSITION('.', amount_cleaned_str_initial) > 0 AND
                 POSITION(',', amount_cleaned_str_initial) = 0
            THEN amount_cleaned_str_initial
            ELSE amount_cleaned_str_initial -- Fallback for cases with no specific separators, or already cleaned.
        END AS amount_to_cast
    FROM
        normalize_whitespace_and_remove_symbols
)

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualifikation', 'qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'needs') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perception') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal', 'price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation', 'review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('won', 'closed won', 'abgeschlossen (gewonnen)', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName if no match found
    END AS "StageName",
    COALESCE(
        -- Attempt to parse YYYY-MM-DD format
        CASE WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END,
        -- Attempt to parse MM/DD/YYYY format
        CASE WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        -- Attempt to parse DD.MM.YYYY format
        CASE WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        -- Attempt to parse YYYYMMDD format
        CASE WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD') ELSE NULL END,
        CURRENT_DATE::TEXT -- Fallback to current date for NOT NULL CloseDate if all parsing attempts fail
    ) AS "CloseDate",
    -- Safely cast the pre-processed amount string to DOUBLE PRECISION
    CASE
        WHEN amount_to_cast ~ '^-?\d+(\.\d+)?$' THEN CAST(amount_to_cast AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    CASE
        WHEN LOWER(TRIM(currencyisocode)) IN ('usd', '$', 'dollar') THEN 'USD'
        WHEN LOWER(TRIM(currencyisocode)) IN ('gbp', '£') THEN 'GBP'
        WHEN LOWER(TRIM(currencyisocode)) IN ('eur', 'euro', '€') THEN 'EUR'
        WHEN LOWER(TRIM(currencyisocode)) IN ('chf') THEN 'CHF'
        ELSE NULL -- CurrencyIsoCode is nullable, so NULL is an acceptable fallback
    END AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate", -- Not available in source, set to NULL as it is nullable
    NULL AS "LastModifiedDate", -- Not available in source, set to NULL as it is nullable
    0 AS "IsDeleted" -- Default to 0 (false) as it is an integer type
FROM
    normalized_amount_for_casting