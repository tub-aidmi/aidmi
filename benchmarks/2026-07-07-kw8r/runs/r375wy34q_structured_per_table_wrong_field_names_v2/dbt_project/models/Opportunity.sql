{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(NULLIF(TRIM(bezeichnung), ''), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(phase)) IN ('prospecting', 'qualification') THEN INITCAP(TRIM(phase))
        WHEN LOWER(TRIM(phase)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(phase)) IN ('value proposition', 'value_proposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(phase)) IN ('id. decision makers', 'id_decision_makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(phase)) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(phase)) IN ('proposal/price quote', 'proposal_price_quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(phase)) IN ('negotiation/review', 'negotiation_review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(phase)) IN ('closed won', 'closed_won') THEN 'Closed Won'
        WHEN LOWER(TRIM(phase)) IN ('closed lost', 'closed_lost') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CASE
        WHEN abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN abschlussdatum
        WHEN abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN abschlussdatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    volumen AS "Amount",
    NULLIF(TRIM(waehrung), '') AS "CurrencyIsoCode",
    kunden.kunden_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON chancen.kd_nr = kunden.kunden_nr