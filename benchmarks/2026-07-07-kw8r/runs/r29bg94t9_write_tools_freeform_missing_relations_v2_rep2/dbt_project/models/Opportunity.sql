{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN TRIM(LOWER(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(o.stage)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(o.stage)) IN ('proposal', 'proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.stage)) IN ('negotiation', 'negotiation/review') THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.stage)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(o.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    '1970-01-01' AS "CloseDate",
    o.amount AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.customer_number AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(REGEXP_REPLACE(LOWER(o.account_name), '[^a-z0-9\s&\.\-]', '', 'g')) = 
       TRIM(REGEXP_REPLACE(LOWER(a.name), '[^a-z0-9\s&\.\-]', '', 'g'))
WHERE a.id IS NOT NULL
