{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    INITCAP(TRIM(o.name)) AS "Name",
    CASE
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) = 'id. decision makers' OR LOWER(TRIM(o.stage)) = 'identify decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL::TEXT AS "CloseDate",
    o.amount AS "Amount",
    'EUR' AS "CurrencyIsoCode",
    CAST(REGEXP_REPLACE(o.customer_number, '^KD-', 'ACC-') AS TEXT) AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
