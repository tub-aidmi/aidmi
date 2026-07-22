-- models/Opportunity.sql
{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, 'Unknown Opportunity') AS "Name", -- Name is NOT NULL
    CASE phase
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for StageName if not matched, as it's NOT NULL
    END AS "StageName",
    COALESCE(abschlussdatum, '1970-01-01') AS "CloseDate", -- CloseDate is NOT NULL, using a sentinel date as fallback
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId", -- Maps to kunden.kunden_nr
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
