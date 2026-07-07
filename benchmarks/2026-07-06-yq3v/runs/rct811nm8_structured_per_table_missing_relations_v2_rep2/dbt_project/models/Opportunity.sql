-- This dbt model transforms raw opportunity data into the target Opportunity schema.
{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(o.stage) LIKE '%prospect%' THEN 'Prospecting'
        WHEN LOWER(o.stage) LIKE '%qualific%' THEN 'Qualification'
        WHEN LOWER(o.stage) LIKE '%needs%' THEN 'Needs Analysis'
        WHEN LOWER(o.stage) LIKE '%value%' THEN 'Value Proposition'
        WHEN LOWER(o.stage) LIKE '%decision%' THEN 'Id. Decision Makers'
        WHEN LOWER(o.stage) LIKE '%percept%' THEN 'Perception Analysis'
        WHEN LOWER(o.stage) LIKE '%proposal%' THEN 'Proposal/Price Quote'
        WHEN LOWER(o.stage) LIKE '%negotiat%' OR LOWER(o.stage) LIKE '%review%' THEN 'Negotiation/Review'
        WHEN LOWER(o.stage) LIKE '%closed won%' THEN 'Closed Won'
        WHEN LOWER(o.stage) LIKE '%closed lost%' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    CURRENT_DATE::TEXT AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NOW()::DATE::TEXT AS "CreatedDate",
    NOW()::DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    o.customer_number = a.id
