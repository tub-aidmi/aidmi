{{ config(materialized='table') }}

SELECT
    '001' || SUBSTRING(MD5(ch.chance_id) FROM 1 FOR 14) AS "Id",
    INITCAP(TRIM(ch.bezeichnung)) AS "Name",
    CASE
        WHEN UPPER(TRIM(ch.phase)) IN ('CLOSED WON', 'CLOSED LOST', 'PROSPECTING', 'QUALIFICATION', 'NEEDS ANALYSIS', 'VALUE PROPOSITION', 'ID. DECISION MAKERS', 'PERCEPTION ANALYSIS', 'PROPOSAL/PRICE QUOTE', 'NEGOTIATION/REVIEW')
        THEN INITCAP(TRIM(ch.phase))
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN ch.abschlussdatum IS NOT NULL AND ch.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN ch.abschlussdatum
        WHEN ch.abschlussdatum IS NOT NULL AND ch.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(ch.abschlussdatum, 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CAST(ch.volumen AS DOUBLE PRECISION) AS "Amount",
    UPPER(TRIM(ch.waehrung)) AS "CurrencyIsoCode",
    '001' || SUBSTRING(MD5(k.kunden_nr) FROM 1 FOR 14) AS "AccountId",
    ch.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} ch
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ch.kd_nr) = TRIM(k.kunden_nr)