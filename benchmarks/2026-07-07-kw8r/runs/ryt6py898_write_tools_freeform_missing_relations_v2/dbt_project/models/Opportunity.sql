{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(NULLIF(TRIM(o.name), ''), 'Untitled Opportunity') AS "Name",
    CASE 
        WHEN LOWER(TRIM(o.stage)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stage)) IN ('qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stage)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('value proposition', 'value_prop') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stage)) IN ('id. decision makers', 'identify decision makers', 'id decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stage)) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stage)) IN ('proposal', 'proposal/price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stage)) IN ('negotiation', 'negotiation/review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stage)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stage)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    NULL AS "CloseDate",
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    COALESCE(a.id, NULL) AS "AccountId",
    o.customer_number AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(o.customer_number) = TRIM(a.id) OR TRIM(o.account_name) = TRIM(a.name)
