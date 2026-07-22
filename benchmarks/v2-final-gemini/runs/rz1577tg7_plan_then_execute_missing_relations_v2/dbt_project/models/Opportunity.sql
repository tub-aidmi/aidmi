{{ config(materialized='table') }}

SELECT
    opportunity.id AS "Id",
    COALESCE(opportunity.name, 'Unknown Opportunity') AS "Name",
    CASE LOWER(opportunity.stage)
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CAST('2000-01-01' AS TEXT) AS "CloseDate",
    opportunity.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    opportunity.customer_number AS "AccountId",
    opportunity.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opportunity
