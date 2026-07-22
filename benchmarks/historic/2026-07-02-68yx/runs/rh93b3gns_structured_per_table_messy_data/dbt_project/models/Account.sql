{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(INITCAP(TRIM("Name")), 'Unknown Account') AS "Name",
    "ERP_Number__c" AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM("Customer_Tier__c")) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM("Customer_Tier__c")) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM("Customer_Tier__c")) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM("Customer_Tier__c")) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM("Region__c")) AS "Region__c",
    INITCAP(TRIM("Industry")) AS "Industry",
    "Website" AS "Website",
    INITCAP(TRIM("BillingCity")) AS "BillingCity",
    INITCAP(TRIM("BillingCountry")) AS "BillingCountry",
    "Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Account') }}