{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, id) AS "Name",
    CASE
        WHEN LOWER(stage) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(stage) = 'qualification' THEN 'Qualification'
        WHEN LOWER(stage) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(stage) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(stage) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(stage) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(stage) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(stage) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CloseDate", -- Placeholder for NOT NULL
    amount AS "Amount",
    'USD' AS "CurrencyIsoCode", -- Default value
    customer_number AS "AccountId", -- Assuming customer_number maps to Account Id
    id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
