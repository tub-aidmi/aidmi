{{ config(materialized='table') }}

SELECT 
    REGEXP_REPLACE(o.id, '^OPP-', '') AS "Id",
    o.name AS "Name",
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
        ELSE INITCAP(TRIM(o.stage))
    END AS "StageName",
    COALESCE(NULL::TEXT, '1900-01-01') AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    REGEXP_REPLACE(a.id, '^ACC-', '') AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
INNER JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON REGEXP_REPLACE(o.customer_number, '^KD-', '') = REGEXP_REPLACE(a.id, '^ACC-', '')