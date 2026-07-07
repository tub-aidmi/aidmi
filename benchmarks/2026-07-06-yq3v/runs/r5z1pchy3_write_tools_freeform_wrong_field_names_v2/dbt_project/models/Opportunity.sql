-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    chance_id AS "Id",
    bezeichnung AS "Name",
    COALESCE(
        CASE
            WHEN lower(phase) = 'prospecting' THEN 'Prospecting'
            WHEN lower(phase) = 'qualification' THEN 'Qualification'
            WHEN lower(phase) = 'needs analysis' THEN 'Needs Analysis'
            WHEN lower(phase) = 'value proposition' THEN 'Value Proposition'
            WHEN lower(phase) = 'id. decision makers' THEN 'Id. Decision Makers'
            WHEN lower(phase) = 'perception analysis' THEN 'Perception Analysis'
            WHEN lower(phase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
            WHEN lower(phase) = 'negotiation/review' THEN 'Negotiation/Review'
            WHEN lower(phase) = 'closed won' THEN 'Closed Won'
            WHEN lower(phase) = 'closed lost' THEN 'Closed Lost'
            ELSE 'Prospecting' -- Default for NOT NULL StageName
        END,
        'Prospecting' -- Ensure a default if phase itself is NULL or not matched
    ) AS "StageName",
    TO_CHAR(CAST(abschlussdatum AS DATE), 'YYYY-MM-DD') AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
