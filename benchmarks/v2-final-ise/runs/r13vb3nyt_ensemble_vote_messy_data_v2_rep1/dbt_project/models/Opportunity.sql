{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    CASE WHEN TRIM(name) = '' THEN NULL ELSE INITCAP(TRIM(name)) END AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'lead gen', 'cold calling', 'initial contact') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifying', 'discovery') THEN 'Qualification'
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
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        -- DD.MM.YYYY format
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' 
            THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
        -- YYYY-MM-DD format
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(TRIM(closedate), 'YYYY-MM-DD')::TEXT
        -- YYYYMMDD format
        WHEN closedate ~ '^\d{8}$'
            THEN TO_DATE(TRIM(closedate), 'YYYYMMDD')::TEXT
        -- MM/DD/YYYY or DD/MM/YYYY — ambiguous, use heuristic on first component
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$'
            THEN CASE 
                WHEN CAST(SUBSTRING(TRIM(closedate) FROM 1 FOR 2) AS INTEGER) > 12 
                    THEN TO_DATE(TRIM(closedate), 'DD/MM/YYYY')::TEXT
                ELSE 
                    TO_DATE(TRIM(closedate), 'MM/DD/YYYY')::TEXT
                END
        ELSE NULL
    END AS "CloseDate",
    -- Amount: robust parsing with explicit European vs US format handling
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE
            CASE
                -- European thousands+dots + comma decimal: e.g. 1.234,56 or 1.234.567,89
                WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '') ~ '^\d{1,3}(\.\d{3})+,\d+$' 
                    THEN REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', ''), '.', ''), ',', '.')::DOUBLE PRECISION
                -- European single dot-comma: e.g. 1234567,89 (no thousands dots) → decimal comma
                WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '') ~ '^\d+,\d+$'
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', ''), ',', '.')::DOUBLE PRECISION
                -- US thousands with commas: e.g. 1,234.56 or plain 1234.56
                WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '') ~ '^\d{1,3}(,\d{3})+\.\d{1,2}$' 
                    THEN CAST(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', ''), ',', '') AS DOUBLE PRECISION)
                -- Plain number with dot (US decimal): e.g. 42543.61 or just an integer
                WHEN REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '') ~ '^\d+\.\d+$' OR REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '') ~ '^\d+$'
                    THEN CAST(REGEXP_REPLACE(TRIM(amount), '[^0-9.,]', '') AS DOUBLE PRECISION)
                ELSE NULL
            END
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    CAST(accountid AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
WHERE id IS NOT NULL