{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER(a.tier)) IN ('gold', 'platinum') THEN INITCAP(TRIM(a.tier))
        WHEN TRIM(LOWER(a.tier)) = 'silver' THEN 'Silver'
        WHEN TRIM(LOWER(a.tier)) = 'bronze' THEN 'Bronze'
        ELSE NULL 
    END AS "Customer_Tier__c",
    TRIM(a.region) AS "Region__c",
    TRIM(a.industry) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    NULL AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"