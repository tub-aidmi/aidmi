{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unnamed Opportunity') AS "Name",
    COALESCE(
        INITCAP(CASE
            WHEN LOWER(TRIM(stagename)) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(TRIM(stagename)) = 'qualification' THEN 'Qualification'
            WHEN LOWER(TRIM(stagename)) = 'needs analysis' THEN 'Needs Analysis'
            WHEN LOWER(TRIM(stagename)) = 'value proposition' THEN 'Value Proposition'
            WHEN LOWER(TRIM(stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(stagename)) = 'perception analysis' THEN 'Perception Analysis'
            WHEN LOWER(TRIM(stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(stagename)) = 'negotiation/review' THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(stagename)) = 'closed won' THEN 'Closed Won'
            WHEN LOWER(TRIM(stagename)) = 'closed lost' THEN 'Closed Lost'
            ELSE INITCAP(TRIM(stagename))
        END),
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        CASE
            WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' THEN NULL
            WHEN closedate ~ '^\d{4}/\d{1,2}/\d{1,2}$' THEN TO_DATE(REGEXP_REPLACE(TRIM(closedate), '/', '-'), 'YYYY-MM-DD')::TEXT
            WHEN closedate ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN TO_DATE(TRIM(closedate), 'YYYY-MM-DD')::TEXT
            WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
            WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(closedate), 'MM/DD/YYYY')::TEXT
            WHEN closedate ~ '^\d{8}$' THEN TO_DATE(TRIM(closedate), 'YYYYMMDD')::TEXT
            WHEN closedate ~ '^\d{1,2}[-/]\d{1,2}[-/]\d{4}$' THEN
                CASE WHEN TRIM(closedate) LIKE '%/%'
                    THEN TO_DATE(TRIM(closedate), 'DD/MM/YYYY')::TEXT
                    ELSE TO_DATE(TRIM(closedate), 'DD-MM-YYYY')::TEXT
                END
            ELSE NULL
        END,
        CURRENT_DATE::TEXT
    ) AS "CloseDate",
    CASE
        WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' THEN 0.0
        -- European format with thousand-separator dots and decimal comma (e.g., "1.234,56")
        WHEN amount ~ '^\s*[€£$]?\s*\d{1,3}(\.\d{3})+,\d{1,2}\s*$'
            THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '^\s*[€£$]\s*', ''), '\.', ''), ',', '.')::DOUBLE PRECISION
        -- European format without thousands but with decimal comma (e.g., "1234,56" or "€1234,56")
        WHEN amount ~ '^\s*[€£$]?\s*\d+,\d{1,2}\s*$'
            THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '^\s*[€£$]\s*', ''), ',', '.')::DOUBLE PRECISION
        -- Standard US format: dots for thousands, period as decimal (e.g., "$1,234.56")
        WHEN amount ~ '^\s*[€£$]?\s*\d[\d,.]*\.\d{1,2}\s*$'
            THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '^\s*[€£$]\s*', ''), ',', '')::DOUBLE PRECISION
        -- Plain integer or whole amount (e.g., "$100" or "€500")
        WHEN amount ~ '^\s*[€£$]?\s*\d+\s*$'
            THEN REGEXP_REPLACE(TRIM(amount), '^\s*[€£$]\s*', '')::DOUBLE PRECISION
        ELSE 0.0
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
WHERE TRIM(id) IS NOT NULL AND TRIM(id) != ''