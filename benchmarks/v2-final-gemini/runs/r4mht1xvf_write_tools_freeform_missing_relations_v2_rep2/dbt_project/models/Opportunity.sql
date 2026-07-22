-- models/Opportunity.sql

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(stage) LIKE '%prospect%' THEN 'Prospecting'
        WHEN LOWER(stage) LIKE '%qualific%' THEN 'Qualification'
        WHEN LOWER(stage) LIKE '%needs analysis%' THEN 'Needs Analysis'
        WHEN LOWER(stage) LIKE '%value propos%' THEN 'Value Proposition'
        WHEN LOWER(stage) LIKE '%decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(stage) LIKE '%perception%' THEN 'Perception Analysis'
        WHEN LOWER(stage) LIKE '%proposal%' OR LOWER(stage) LIKE '%price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(stage) LIKE '%negotiation%' OR LOWER(stage) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(stage) LIKE '%won%' THEN 'Closed Won'
        WHEN LOWER(stage) LIKE '%lost%' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default stage for unmapped or NULL values
    END AS "StageName",
    '2000-01-01' AS "CloseDate", -- Target column is NOT NULL, providing a sensible default.
    amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    NULL AS "AccountId", -- Cannot directly map to Salesforce Account Id without joining to Account model or external mapping.
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
