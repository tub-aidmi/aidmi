{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(name, ''), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(tier)) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(TRIM(tier)) IN ('silver') THEN 'Silver'
        WHEN LOWER(TRIM(tier)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(tier)) IN ('platinum', 'plat') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region) AS "Region__c",
    TRIM(industry) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
