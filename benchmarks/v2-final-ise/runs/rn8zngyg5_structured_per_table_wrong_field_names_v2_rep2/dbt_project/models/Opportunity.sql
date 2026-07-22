{{ config(materialized='table') }}

SELECT
    CAST(ch.chance_id AS TEXT) AS "Id",
    INITCAP(TRIM(ch.bezeichnung)) AS "Name",
    CASE 
        WHEN LOWER(TRIM(ch.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(ch.phase)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(ch.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(ch.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(ch.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(ch.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(ch.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(ch.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(ch.phase)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(ch.phase)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN ch.abschlussdatum IS NOT NULL AND TRIM(ch.abschlussdatum) != ''
            THEN TO_DATE(TRIM(ch.abschlussdatum), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    CAST(ch.volumen AS DOUBLE PRECISION) AS "Amount",
    UPPER(TRIM(ch.waehrung)) AS "CurrencyIsoCode",
    LOWER(TRIM(k.kunden_nr)) AS "AccountId",
    ch.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} ch
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ch.kd_nr) = TRIM(k.kunden_nr)