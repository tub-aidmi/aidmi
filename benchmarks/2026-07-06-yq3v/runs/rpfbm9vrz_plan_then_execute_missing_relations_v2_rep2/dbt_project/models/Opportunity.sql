{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'N/A') AS "Name",
    CASE
        WHEN LOWER(o.stage) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(o.stage) = 'qualification' THEN 'Qualification'
        WHEN LOWER(o.stage) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(o.stage) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(o.stage) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(o.stage) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.customer_number = a.id
