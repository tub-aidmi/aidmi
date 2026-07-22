{{ config(materialized='table') }}

WITH opp AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}
),
acct AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)
SELECT 
    CAST(o.id AS TEXT) AS "Id",
    INITCAP(TRIM(o.name)) AS "Name",
    CASE LOWER(TRIM(o.stage))
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
        ELSE NULL
    END AS "StageName",
    NULL::TEXT AS "CloseDate",
    CAST(o.amount AS DOUBLE PRECISION) AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    CAST(a.id AS TEXT) AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM opp o
LEFT JOIN acct a 
    ON TRIM(LOWER(o.customer_number)) = TRIM(LOWER(a.id))