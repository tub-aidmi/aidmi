{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(o.stage) ILIKE 'Prospecting' THEN 'Prospecting'
        WHEN TRIM(o.stage) ILIKE 'Qualification' THEN 'Qualification'
        WHEN TRIM(o.stage) ILIKE 'Needs Analysis' THEN 'Needs Analysis'
        WHEN TRIM(o.stage) ILIKE 'Value Proposition' THEN 'Value Proposition'
        WHEN TRIM(o.stage) ILIKE 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN TRIM(o.stage) ILIKE 'Perception Analysis' THEN 'Perception Analysis'
        WHEN TRIM(o.stage) ILIKE 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(o.stage) ILIKE 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN TRIM(o.stage) ILIKE 'Closed Won' THEN 'Closed Won'
        WHEN TRIM(o.stage) ILIKE 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate",
    o.amount AS "Amount",
    NULL::text AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    REGEXP_REPLACE(o.customer_number, '^KD-', 'ACC-') = a.id
