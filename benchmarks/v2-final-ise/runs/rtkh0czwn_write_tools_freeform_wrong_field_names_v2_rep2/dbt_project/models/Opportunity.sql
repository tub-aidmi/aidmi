{{ config(materialized='table') }}

SELECT
    c.chance_id AS "Id",
    TRIM(c.bezeichnung) AS "Name",
    CASE 
        WHEN TRIM(c.phase) = 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(c.phase) = 'Qualification' THEN 'Qualification'
        WHEN TRIM(c.phase) = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(c.phase) = 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(c.phase) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(c.phase) = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(c.phase) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(c.phase) = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(c.phase) = 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(c.phase) = 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN c.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN c.abschlussdatum
        ELSE NULL
    END AS "CloseDate",
    c.volumen AS "Amount",
    TRIM(c.waehrung) AS "CurrencyIsoCode",
    c.kd_nr AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
