{{ config(materialized='table') }}

SELECT 
    -- Id: Concatenate SFDC prefix 'a06' with trimmed source key
    CONCAT('a06', TRIM(chancen.chance_id)) AS "Id",
    
    -- Name: INITCAP, TRIM. NOT NULL handling for empty/nulls.
    COALESCE(
        INITCAP(TRIM(NULLIF(chancen.bezeichnung, ''))), 
         'Unknown Opportunity'
     ) AS "Name",

    -- StageName: Map source enum to target pipeline stages (case-insensitive). Fallback 'Prospecting'.
    CASE 
        WHEN LOWER(TRIM(chancen.phase)) IN ('prospecting', 'akquise') THEN 'Prospecting'
        WHEN LOWER(TRIM(chancen.phase)) IN ('qualification', 'qualifikation') THEN 'Qualification'
        WHEN LOWER(TRIM(chancen.phase)) IN ('needs analysis', 'bedarfsanalyse') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(chancen.phase)) IN ('value proposition', 'wertversprechen') THEN 'Value Proposition'
        WHEN LOWER(TRIM(chancen.phase)) IN ('id. decision makers', 'identifizierung entscheidungsträger') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(chancen.phase)) IN ('perception analysis', 'wahrnehmungsanalyse') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(chancen.phase)) IN ('proposal/price quote', 'price quote', 'angebot', 'proposal / price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(chancen.phase)) IN ('negotiation/review', 'negotiation / review', 'verhandlung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(chancen.phase)) IN ('closed won', 'gewonnen') THEN 'Closed Won'
        WHEN LOWER(TRIM(chancen.phase)) IN ('closed lost', 'verloren') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Fallback for unmapped values to satisfy NOT NULL constraint
    END AS "StageName",

    -- CloseDate: Parse date string (DD.MM.YYYY, YYYYMMDD) -> YYYY-MM-DD. Return NULL if unparseable.
    CASE 
        WHEN TRIM(chancen.abschlussdatum) IS NULL THEN NULL
        WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(chancen.abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN chancen.abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(chancen.abschlussdatum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(chancen.abschlussdatum) -- Format already YYYY-MM-DD
        ELSE NULL
    END AS "CloseDate",

    -- Amount: Source column is already double precision — no regex needed; just cast explicitly.
    CAST(chancen.volumen AS DOUBLE PRECISION) AS "Amount",

    -- CurrencyIsoCode: Extract alphabetic characters and uppercase. Remove symbols like '€'.
    UPPER(REGEXP_REPLACE(TRIM(chancen.waehrung), '[^A-Za-z]', '', 'g')) AS "CurrencyIsoCode",

    -- AccountId: Resolve via join with source kunden table to map kd_nr -> kunden_nr -> 'a00' prefix Id.
    CASE 
        WHEN TRIM(kunden.kunden_nr) IS NOT NULL THEN CONCAT('a00', TRIM(kunden.kunden_nr))
        ELSE NULL
    END AS "AccountId",

    -- Legacy_Opportunity_ID__c: Source natural key
    TRIM(chancen.chance_id) AS "Legacy_Opportunity_ID__c",

    -- CreatedDate / LastModifiedDate: Current date as text.
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",

    -- IsDeleted: Constant 0
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} chancen
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} kunden 
    ON TRIM(chancen.kd_nr) = TRIM(kunden.kunden_nr)