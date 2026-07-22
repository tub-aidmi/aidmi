{{ config(materialized='table') }}

SELECT 
    SPLIT_PART(TRIM(id), '-', 2) AS "Id",
    name AS "Name",
    NULL AS "ERP_Number__c",
    CASE WHEN LOWER(tier) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(tier) ELSE NULL END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    TRIM(id) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}