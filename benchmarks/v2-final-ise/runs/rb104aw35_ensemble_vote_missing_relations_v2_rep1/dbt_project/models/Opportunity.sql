{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    TRIM(o.name) AS "Name",
    CASE 
        WHEN LOWER(TRIM(o.stage)) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) = 'qualification' THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) = 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CAST('1970-01-01' AS TEXT) AS "CloseDate",
    o.amount AS "Amount",
    CAST('EUR' AS TEXT) AS "CurrencyIsoCode",
    CASE 
        WHEN o.customer_number IS NOT NULL AND o.customer_number ~ '^KD-\d+$'
        THEN 'ACC-' || SPLIT_PART(TRIM(o.customer_number), 'KD-', 2)
        ELSE NULL
    END AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    CAST('1970-01-01' AS TEXT) AS "CreatedDate",
    CAST('1970-01-01' AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o