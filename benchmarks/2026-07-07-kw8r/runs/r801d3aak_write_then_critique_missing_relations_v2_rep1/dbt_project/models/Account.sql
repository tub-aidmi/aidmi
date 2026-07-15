{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(a.tier)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(a.tier)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(a.tier)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(a.tier)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL 
    END AS "Customer_Tier__c",
    TRIM(a.region) AS "Region__c",
    TRIM(a.industry) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    a.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"