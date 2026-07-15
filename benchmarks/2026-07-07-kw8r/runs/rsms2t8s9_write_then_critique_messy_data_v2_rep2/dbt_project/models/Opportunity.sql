{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(name, 'Unnamed Opportunity'))) AS "Name",

    -- Map source stagename to normalized enum values; fallback to 'Qualification' for unmapped values (satisfies NOT NULL CHECK constraint)
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qual') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'need analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) LIKE '%decision maker%' OR LOWER(TRIM(stagename)) LIKE '%identify decision%' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) LIKE '%perception%analysis%' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) LIKE '%proposal%' OR LOWER(TRIM(stagename)) LIKE '%quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) LIKE '%negotiation%' OR LOWER(TRIM(stagename)) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE 'Qualification'   -- fallback to valid enum member per CHECK constraint; source value is unrecognised
    END AS "StageName",

    -- Parse CloseDate from multiple text formats into ISO YYYY-MM-DD; default to placeholder for NULL/unparseable
    COALESCE(
        CASE 
            WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' THEN NULL
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
            WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
            WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
            ELSE NULL
        END,
        '1900-01-01'   -- NOT NULL-safe sentinel; swap to preferred default if business logic changes
    ) AS "CloseDate",

    -- Clean Amount: handle European (dot-thousands, comma-decimal) and US formats; POSIX regex only
    CASE 
        WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' THEN CAST(NULL AS DOUBLE PRECISION)
        ELSE
            REGEXP_REPLACE(
                CASE
                    -- European format: both dot and comma present, comma after last dot → comma is decimal separator
                    WHEN position(',' IN amount) > 0
                         AND position('.' IN amount) > 0
                         AND position(',' IN amount) > position('.' IN amount)
                        THEN REPLACE(REPLACE(amount, '.', ''), ',', '.')
                    -- Commas as thousands separators (US or similar); remove them entirely
                    WHEN position(',' IN amount) > 0
                        THEN REGEXP_REPLACE(amount, ',', '', 'g')
                    ELSE amount
                END
                -- Strip any remaining non-numeric characters except digits, dot, and leading minus
                , '[^\d.\-]', '', 'g'
            )::DOUBLE PRECISION
    END AS "Amount",

    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",

    -- Fields not present in source — default to safe values
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }};