
{{ config(materialized='table') }}

SELECT
    opp.id AS "Id",
    COALESCE(opp.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(opp.stage) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(opp.stage) = 'qualification' THEN 'Qualification'
        WHEN LOWER(opp.stage) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(opp.stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(opp.stage) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(opp.stage) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(opp.stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(opp.stage) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(opp.stage) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(opp.stage) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    opp.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    opp.customer_number AS "AccountId",
    NULL AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Opportunity') }} AS opp
