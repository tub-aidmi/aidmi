{{ config(materialized='table') }}

WITH source AS (
    SELECT
        c.chance_id,
        c.bezeichnung,
        c.phase,
        c.abschlussdatum,
        c.volumen,
        c.waehrung,
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    INNER JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
        ON c.kd_nr = k.kunden_nr
)

SELECT
    -- Salesforce-style Opportunity Id (15-char format): '006' + padded numeric from chance_id
    '006' || LPAD(
        SUBSTRING(chance_id FROM '\d+')::INTEGER::TEXT, 12, '0'
    ) AS "Id",

    -- Name: trimmed and initcap'd opportunity description
    INITCAP(TRIM(bezeichnung)) AS "Name",

    -- StageName: source phase values mapped to target enum domain
    CASE LOWER(TRIM(phase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",

    -- CloseDate: multi-format handling for YYYY-MM-DD and YYYYMMDD, output ISO text; NULL if unparseable
    CASE
        WHEN abschlussdatum IS NULL THEN NULL
        WHEN abschlussdatum ~ '^\d{8}$' THEN
            TO_DATE(abschlussdatum, 'YYYYMMDD')::TEXT
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN
            TO_DATE(abschlussdatum, 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "CloseDate",

    -- Amount: direct from double precision volumen column
    volumen AS "Amount",

    -- CurrencyIsoCode: EUR, GBP, CHF, USD — all valid ISO 4217 codes
    UPPER(TRIM(waehrung)) AS "CurrencyIsoCode",

    -- AccountId: Salesforce-style Account Id derived from customer number using canonical transform '001' + LPAD(...,12,'0')
    -- Matches Account.Id definition exactly for cross-table referential integrity
    '001' || LPAD(
        SUBSTRING(kunden_nr FROM '\d+')::INTEGER::TEXT, 12, '0'
    ) AS "AccountId",

    -- Legacy_Opportunity_ID__c: direct from source natural key
    chance_id AS "Legacy_Opportunity_ID__c",

    -- CreatedDate: NULL since not present in source data (prefer NULL over sentinel)
    CAST(NULL AS TEXT) AS "CreatedDate",

    -- LastModifiedDate: use the parsed close date as a proxy; NULL if source is missing
    CASE
        WHEN abschlussdatum IS NULL THEN NULL
        WHEN abschlussdatum ~ '^\d{8}$' THEN
            TO_DATE(abschlussdatum, 'YYYYMMDD')::TEXT
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN
            TO_DATE(abschlussdatum, 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "LastModifiedDate",

    -- Not deleted
    0 AS "IsDeleted"

FROM source