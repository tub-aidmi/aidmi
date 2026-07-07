-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    bezeichnung AS "Name",
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
        ELSE NULL
    END AS "StageName",
    TO_CHAR(CAST(abschlussdatum AS DATE), 'YYYY-MM-DD') AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId", -- Assuming 'kd_nr' is the kunden_nr from the Account table
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
