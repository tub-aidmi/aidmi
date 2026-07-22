{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(name, ''), 'Unknown') AS "Name",
    id AS "Legacy_Customer_ID__c",
    CASE
        WHEN LOWER(tier) IN ('gold', 'g') THEN 'Gold'
        WHEN LOWER(tier) IN ('silver', 's') THEN 'Silver'
        WHEN LOWER(tier) IN ('bronze', 'b') THEN 'Bronze'
        WHEN LOWER(tier) IN ('platinum', 'p') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
