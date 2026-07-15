{{ config(materialized='table') }}

SELECT 
    TRIM(UPPER(id)) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Untitled Opportunity') AS "Name",
    CASE INITCAP(TRIM(stage))
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Needs Analysis' THEN 'Needs Analysis'
        WHEN 'Value Proposition' THEN 'Value Proposition'
        WHEN 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN 'Perception Analysis' THEN 'Perception Analysis'
        WHEN 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN 'Closed Won' THEN 'Closed Won'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    '1900-01-01' AS "CloseDate",
    CAST(amount AS DOUBLE PRECISION) AS "Amount",
    'USD' AS "CurrencyIsoCode",
    'ACC-' || REGEXP_REPLACE(TRIM(customer_number), '\D', '', 'g') AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}