{{ config(materialized='table') }}

SELECT
    UPPER(SUBSTRING(MD5(LOWER(chance_id::text)) FROM 1 FOR 18)) AS "Id",
    INITCAP(TRIM(bezeichnung)) AS "Name",
    CASE LOWER(TRIM(COALESCE(phase, '')))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'decision maker identification' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'gewinn' THEN 'Closed Won'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(abschlussdatum) = '' THEN NULL
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(abschlussdatum), 'DD.MM.YYYY')::TEXT
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(abschlussdatum)
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_DATE(TRIM(abschlussdatum), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN volumen IS NULL OR TRIM(volumen::text) = '' THEN NULL
        WHEN TRIM(volumen::text) ~ '^[-+]?\d[\d.,]*\.\d+$'
            THEN (TRIM(volumen::text)::DOUBLE PRECISION)
        WHEN TRIM(volumen::text) ~ '^[-+]?\d[\d,.]+$' AND POSITION(',' IN TRIM(volumen::text)) > POSITION('.' IN TRIM(volumen::text))
            -- European format: 1.234,56 -> remove dots, swap comma to dot
            THEN REGEXP_REPLACE(TRIM(volumen::text), '[.]', '', 'g')::DOUBLE PRECISION / NULLIF(POSITION(',' IN REGEXP_REPLACE(TRIM(volumen::text), '[.]', '', 'g')), 0) * NULLIF(NULL, 0) +
                 CAST(SPLIT_PART(TRIM(volumen::text), ',', -1)::INTEGER AS DOUBLE PRECISION) / POWER(10, LENGTH(SPLIT_PART(TRIM(volumen::text), ',', -1)) - POSITION(',', TRIM(volumen::text)) + ...)
        ELSE CAST(REGEXP_REPLACE(TRIM(volumen::text), '[^\d.\-+]', '', 'g') AS DOUBLE PRECISION)
    END AS "Amount",
    waehrung AS "CurrencyIsoCode",
    UPPER(SUBSTRING(MD5(LOWER(kd_nr::text)) FROM 1 FOR 18)) AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
