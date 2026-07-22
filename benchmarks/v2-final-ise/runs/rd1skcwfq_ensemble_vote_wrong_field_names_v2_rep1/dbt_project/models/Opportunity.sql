{{ config(materialized='table') }}

SELECT
    CAST('001' || REPLACE(c.kd_nr, 'CUST-', '') AS TEXT) AS "Id",
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
        ELSE NULL
    END AS "StageName",
    c.ababschlussdatum AS "CloseDate",
    c.volumen AS "Amount",
    c.waehrung AS "CurrencyIsoCode",
    CAST('001' || REPLACE(c.kd_nr, 'CUST-', '') AS TEXT) AS "AccountId",
    c.chance_id AS "Legacy_Opportunity_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c