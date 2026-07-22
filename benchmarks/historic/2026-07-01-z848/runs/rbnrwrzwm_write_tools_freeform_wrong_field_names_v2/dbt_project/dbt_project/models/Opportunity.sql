{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    COALESCE(bezeichnung, 'Unknown Opportunity') AS "Name", -- Name is NOT NULL
    COALESCE(
        CASE
            WHEN TRIM(phase) = 'Prospecting' THEN 'Prospecting'
            WHEN TRIM(phase) = 'Qualification' THEN 'Qualification'
            WHEN TRIM(phase) = 'Needs Analysis' THEN 'Needs Analysis'
            WHEN TRIM(phase) = 'Value Proposition' THEN 'Value Proposition'
            WHEN TRIM(phase) = 'Id. Decision Makers' THEN 'Id. Decision Makers'
            WHEN TRIM(phase) = 'Perception Analysis' THEN 'Perception Analysis'
            WHEN TRIM(phase) = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
            WHEN TRIM(phase) = 'Negotiation/Review' THEN 'Negotiation/Review'
            WHEN TRIM(phase) = 'Closed Won' THEN 'Closed Won'
            WHEN TRIM(phase) = 'Closed Lost' THEN 'Closed Lost'
            ELSE 'Prospecting'
        END, 'Prospecting'
    ) AS "StageName",
    COALESCE(
        TO_CHAR(TO_DATE(abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        '1900-01-01'
    ) AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
