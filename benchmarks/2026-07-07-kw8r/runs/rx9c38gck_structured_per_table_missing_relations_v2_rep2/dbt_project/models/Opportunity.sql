{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(o.stage) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(o.stage) IN ('qualification') THEN 'Qualification'
        WHEN LOWER(o.stage) IN ('needs analysis') THEN 'Needs Analysis'
        WHEN LOWER(o.stage) IN ('value proposition') THEN 'Value Proposition'
        WHEN LOWER(o.stage) IN ('id. decision makers', 'identify decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) IN ('perception analysis') THEN 'Perception Analysis'
        WHEN LOWER(o.stage) IN ('proposal/price quote', 'proposal') THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) IN ('negotiation/review', 'negotiation') THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) IN ('closed won') THEN 'Closed Won'
        WHEN LOWER(o.stage) IN ('closed lost') THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    '2025-12-31' AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    COALESCE(a.id, '001000000000000') AS "AccountId",
    o.customer_number AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON o.account_name = a.name OR o.customer_number = a.id