{{ config(materialized='table') }}
SELECT
    LEFT(MD5(c.chance_id::text), 18) AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN TRIM(LOWER(c.phase)) IN ('prospecting') THEN 'Prospecting'
        WHEN TRIM(LOWER(c.phase)) IN ('qualification') THEN 'Qualification'
        WHEN TRIM(LOWER(c.phase)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN TRIM(LOWER(c.phase)) IN ('value proposition', 'value_proposition') THEN 'Value Proposition'
        WHEN TRIM(LOWER(c.phase)) IN ('id. decision makers', 'id_decision_makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(c.phase)) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
        WHEN TRIM(LOWER(c.phase)) IN ('proposal/price quote', 'proposal_price_quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(c.phase)) IN ('negotiation/review', 'negotiation_review', 'negotiation') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(c.phase)) IN ('closed won', 'closed_won') THEN 'Closed Won'
        WHEN TRIM(LOWER(c.phase)) IN ('closed lost', 'closed_lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN TRIM(c.abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(c.abschlussdatum)
        WHEN TRIM(c.abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(c.abschlussdatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(c.abschlussdatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(c.abschlussdatum) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(c.abschlussdatum), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
    TRIM(c.waehrung) AS "CurrencyIsoCode",
    LEFT(MD5(k.kunden_nr::text), 18) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON c.kd_nr = k.kunden_nr