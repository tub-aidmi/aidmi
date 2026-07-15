{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE 
        WHEN TRIM(LOWER(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(o.stage)) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(o.stage)) IN ('proposal', 'proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.stage)) IN ('negotiation', 'negotiation/review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.stage)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN TRIM(LOWER(o.stage)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON o.customer_number = a.id OR o.account_name = a.name
