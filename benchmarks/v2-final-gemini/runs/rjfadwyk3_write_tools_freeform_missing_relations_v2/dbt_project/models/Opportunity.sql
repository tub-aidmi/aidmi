{{
    config(materialized='table')
}}

WITH source_data AS (
    SELECT
        id,
        name,
        stage,
        amount,
        customer_number
    FROM
        {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
)

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Opportunity') AS "Name",
    COALESCE(CASE
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
        ELSE 'Prospecting' -- Default for NOT NULL enum
    END, 'Prospecting') AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate", -- NOT NULL, no source provided, using current date as default
    amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    customer_number AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_data