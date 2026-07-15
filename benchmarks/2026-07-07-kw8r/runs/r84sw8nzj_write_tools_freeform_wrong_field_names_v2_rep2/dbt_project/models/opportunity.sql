{{ config(materialized='table') }}

SELECT
    '006' || LPAD(REGEXP_REPLACE(chance_id, '\D', '', 'g'), 12, '0') AS "Id",
    bezeichnung AS "Name",
    CASE phase
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Permission Analysis' THEN 'Permission Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    abschlussdatum AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    '001' || LPAD(REGEXP_REPLACE(kd_nr, '\D', '', 'g'), 12, '0') AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
