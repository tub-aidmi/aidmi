{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(INITCAP(TRIM("Name")), 'Unknown Account') AS "Name",
    TRIM("ERP_Number__c") AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM("Customer_Tier__c")) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM("Customer_Tier__c")) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM("Customer_Tier__c")) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM("Customer_Tier__c")) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM("Region__c")) AS "Region__c",
    INITCAP(TRIM("Industry")) AS "Industry",
    TRIM("Website") AS "Website",
    "BillingCity" AS "BillingCity",
    "BillingCountry" AS "BillingCountry",
    TRIM("Legacy_Customer_ID__c") AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Account') }}