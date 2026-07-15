{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
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
        ELSE NULL
    END AS "StageName",
    COALESCE(
        CASE 
            WHEN o.stage IN ('Closed Won', 'Closed Lost') THEN '2025-01-01'
            ELSE '1970-01-01'
        END,
        '1970-01-01'
    ) AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.customer_number AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON o.account_name = a.name
