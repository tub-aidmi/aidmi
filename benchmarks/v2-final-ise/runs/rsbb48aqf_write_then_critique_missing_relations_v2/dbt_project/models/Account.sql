{{ config(materialized='table') }}
SELECT 
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER(tier)) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(TRIM(LOWER(tier)))
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