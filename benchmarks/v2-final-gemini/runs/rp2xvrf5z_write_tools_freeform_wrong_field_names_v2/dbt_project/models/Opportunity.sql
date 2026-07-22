-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    chancen.chance_id AS "Id",
    COALESCE(chancen.bezeichnung, chancen.chance_id) AS "Name",
    COALESCE(
        CASE
            WHEN LOWER(TRIM(chancen.phase)) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(TRIM(chancen.phase)) = 'qualification' THEN 'Qualification'
            WHEN LOWER(TRIM(chancen.phase)) = 'needs analysis' THEN 'Needs Analysis'
            WHEN LOWER(TRIM(chancen.phase)) = 'value proposition' THEN 'Value Proposition'
            WHEN LOWER(TRIM(chancen.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(chancen.phase)) = 'perception analysis' THEN 'Perception Analysis'
            WHEN LOWER(TRIM(chancen.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(chancen.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(chancen.phase)) = 'closed won' THEN 'Closed Won'
            WHEN LOWER(TRIM(chancen.phase)) = 'closed lost' THEN 'Closed Lost'
            ELSE 'Prospecting'
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        TO_CHAR(NULLIF(TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(chancen.abschlussdatum, 'YYYYMMDD'), '0001-01-01'), 'YYYY-MM-DD'),
        '2000-01-01'
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    chancen.waehrung AS "CurrencyIsoCode",
    chancen.kd_nr AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
