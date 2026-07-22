{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(id)) AS "Id",
    INITCAP(NULLIF(Trim(name), '')) AS "Name",
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
    CAST(NULLIF(TRIM(CAST(amount AS TEXT)), '') AS DOUBLE PRECISION) AS "Amount",
    'USD' AS "CurrencyIsoCode",
    UPPER(TRIM(customer_number)) AS "AccountId",
    TRIM(id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }}