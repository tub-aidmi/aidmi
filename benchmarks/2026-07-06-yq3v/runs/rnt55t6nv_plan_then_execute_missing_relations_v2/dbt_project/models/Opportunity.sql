{{ config(materialized='table') }}

WITH opportunities AS (
    SELECT
        opportunity.id,
        opportunity.name,
        opportunity.stage,
        opportunity.amount,
        opportunity.customer_number,
        account.id AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opportunity
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
        ON opportunity.customer_number = account.id
)
SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stage)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stage)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stage)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    account_id AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opportunities