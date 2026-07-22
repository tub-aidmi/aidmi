{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    CASE
        WHEN LOWER(TRIM(stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualification', 'qualifying') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'needs-analysis', 'needanalysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value-proposition', 'valueproposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('id. decision makers', 'id decision makers', 'identify decision makers', 'decision maker identification') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perception-analysis', 'perceptionanalysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal price quote', 'proposalphasequote', 'quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation review', 'negotiationreview', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(closedate) IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN TRIM(closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(closedate), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(closedate)
        WHEN TRIM(closedate) ~ '^\d{8}$' THEN SUBSTR(TRIM(closedate), 1, 4) || '-' || SUBSTR(TRIM(closedate), 5, 2) || '-' || SUBSTR(TRIM(closedate), 7, 2)
        WHEN TRIM(closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(closedate), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(closedate) ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(TRIM(closedate), 'DD-MM-YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(amount) IS NULL OR TRIM(amount) = '' THEN NULL
        WHEN CAST(REGEXP_REPLACE(REPLACE(REPLACE(TRIM(amount), '$', ''), '€', ''), ',', '') AS DOUBLE PRECISION) > 0 AND REGEXP_REPLACE(TRIM(amount), '[^.]', '') ~ '\.$' THEN
            CASE
                WHEN LENGTH(REPLACE(REGEXP_REPLACE(TRIM(amount), '[^0-9,]', '')) - LENGTH(REPLACE(REPLACE(TRIM(amount), '.', ''), ',', ''))) >= 4 THEN
                    -- European format with dot as thousand separator and comma as decimal: e.g. "1.234,56"
                    CAST(REGEXP_REPLACE(REPLACE(TRIM(amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
                ELSE
                    CAST(REGEXP_REPLACE(REPLACE(TRIM(amount), '€', ''), '$', '') AS DOUBLE PRECISION)
            END
        WHEN TRIM(amount) ~ '.*\..*,' THEN
            -- European format: e.g. "1234,56" or "1.234,56" — replace dots (thousand sep), swap comma to dot
            CAST(REGEXP_REPLACE(REPLACE(TRIM(amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(amount) ~ '^[^0-9]*[0-9]+,[0-9]+$' THEN
            -- European format without thousand separator: e.g. "1234,56"
            CAST(REPLACE(TRIM(amount), ',', '.') AS DOUBLE PRECISION)
        ELSE
            -- US/standard format: strip any currency symbols and cast
            CAST(REGEXP_REPLACE(TRIM(amount), '[^0-9.\-+]', '') AS DOUBLE PRECISION)
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    TRIM(accountid) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
