{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE 
        WHEN name IS NOT NULL AND TRIM(name) != '' THEN INITCAP(TRIM(name)) 
        ELSE 'Unknown' 
    END AS "Name",
    NULL AS "ERP_Number__c",
    CASE 
        WHEN LOWER(tier) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(TRIM(tier))
        ELSE 'Bronze' 
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}