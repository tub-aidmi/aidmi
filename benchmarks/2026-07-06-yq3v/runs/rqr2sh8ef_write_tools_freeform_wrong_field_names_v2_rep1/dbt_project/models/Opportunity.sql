{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, 'Unknown Opportunity') AS "Name", -- Name is NOT NULL
    CASE
        WHEN phase = 'Prospecting' THEN 'Prospecting'
        WHEN phase = 'Qualification' THEN 'Qualification'
        WHEN phase = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN phase = 'Value Proposition' THEN 'Value Proposition'
        WHEN phase = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN phase = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN phase = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN phase = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN phase = 'Closed Won' THEN 'Closed Won'
        WHEN phase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for StageName which is NOT NULL
    END AS "StageName",
    COALESCE(TO_CHAR(TO_DATE(abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'), '1900-01-01') AS "CloseDate", -- CloseDate is NOT NULL, default to an early date
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId", -- Corresponds to Account.Id (kunden_nr)
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
