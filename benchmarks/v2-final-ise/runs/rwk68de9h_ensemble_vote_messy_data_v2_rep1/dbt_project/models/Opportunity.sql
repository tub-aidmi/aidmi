{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    CASE WHEN TRIM(name) = '' THEN NULL ELSE INITCAP(TRIM(name)) END AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'lead gen', 'cold calling', 'initial contact') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifying', 'discovery', 'needs assessment') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'needs assessment', 'requirements analysis', 'analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value prop', 'proposal draft') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'identifying decision makers', 'decision maker identification', 'decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perception check', 'competitor analysis', 'evaluation') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'quote', 'proposal', 'proprice quote', 'pricing proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation', 'review', 'contract review', 'final negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won', 'closed_won', 'won deal', 'signed') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost', 'closed_lost', 'loss', 'not won') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        -- Try DD.MM.YYYY format (e.g., 15.03.2024)
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        -- Try YYYY-MM-DD format (e.g., 2024-03-15)
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
        -- Try MM/DD/YYYY format (e.g., 03/15/2024)
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
        -- Try YYYYMMDD format (e.g., 20240315)
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
        -- Try DD/MM/YYYY format (e.g., 15/03/2024)
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' AND SUBSTRING(closedate FROM 1 FOR 2)::INTEGER > 12 THEN TO_DATE(closedate, 'DD/MM/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        WHEN amount ~ '^\s*[€$£]\s*[\d.]+,[\d]+\s*$' THEN
            -- European format with currency symbol: 1.234,56 or €1.234,56
            (REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d,.]', '', 'g'), '\.', '', 'g')::DOUBLE PRECISION / 100) * 100
                - MOD((REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d,.]', '', 'g'), '\.', '', 'g')::DOUBLE PRECISION), 1)
                + (MOD((REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d,.]', '', 'g'), '\.', '', 'g')::DOUBLE PRECISION), 1))::INTEGER * 0.01
            -- Simpler approach: remove dots (thousand separators), swap comma to dot
            REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d.,]', '', 'g'), '\.', '')::TEXT || '.' ||
            REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d.,]', '', 'g'), '.*,', ',')
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
WHERE id IS NOT NULL