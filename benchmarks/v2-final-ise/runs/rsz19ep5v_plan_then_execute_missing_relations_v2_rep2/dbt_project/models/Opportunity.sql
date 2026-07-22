{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(id)) AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    CASE 
        WHEN LOWER(TRIM(stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(stage)) LIKE 'id. decision makers%' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stage)) LIKE 'proposal/price quote%' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stage)) LIKE 'negotiation/review%' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    CAST(amount AS DOUBLE PRECISION) AS "Amount",
    'USD' AS "CurrencyIsoCode",
    'ACC-' || LPAD(SUBSTRING(TRIM(customer_number) FROM '\d+'), 4, '0') AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}