{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, chance_id) AS "Name",
    COALESCE(
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
            ELSE 'Prospecting' -- Default if phase is unexpected or NULL
        END, 'Prospecting') AS "StageName",
    COALESCE(TO_CHAR(CAST(abschlussdatum AS DATE), 'YYYY-MM-DD'), TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')) AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
