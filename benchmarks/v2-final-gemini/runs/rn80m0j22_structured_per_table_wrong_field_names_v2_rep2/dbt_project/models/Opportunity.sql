{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    c.bezeichnung AS "Name",
    CASE
        WHEN c.phase = 'Prospecting' THEN 'Prospecting'
        WHEN c.phase = 'Qualification' THEN 'Qualification'
        WHEN c.phase = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN c.phase = 'Value Proposition' THEN 'Value Proposition'
        WHEN c.phase = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN c.phase = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN c.phase = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN c.phase = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN c.phase = 'Closed Won' THEN 'Closed Won'
        WHEN c.phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting if phase is unknown/null
    END AS "StageName",
    c.abschlussdatum AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    c.kd_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
