{{ config(materialized='table') }}

SELECT
    src_opportunity.id AS "Id",
    COALESCE(src_opportunity.name, 'Unknown') AS "Name",
    CASE
        WHEN LOWER(src_opportunity.stage) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(src_opportunity.stage) = 'qualification' THEN 'Qualification'
        WHEN LOWER(src_opportunity.stage) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(src_opportunity.stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(src_opportunity.stage) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(src_opportunity.stage) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(src_opportunity.stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(src_opportunity.stage) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(src_opportunity.stage) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(src_opportunity.stage) = 'closed lost' THEN 'Closed Lost'
        WHEN LOWER(src_opportunity.stage) = 'won' THEN 'Closed Won'
        WHEN LOWER(src_opportunity.stage) = 'lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default as per plan, since it's NOT NULL
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate", -- No source, default to current date, cast to text
    src_opportunity.amount AS "Amount",
    NULL AS "CurrencyIsoCode", -- No source field, default to NULL
    src_opportunity.customer_number AS "AccountId", -- Directly mapped as per plan
    src_opportunity.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS src_opportunity
