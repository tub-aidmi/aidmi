{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE(INITCAP(TRIM("Name")), 'Unnamed Account') AS "Name",
    TRIM("ERP_Number__c") AS "ERP_Number__c",
    CASE LOWER(TRIM("Customer_Tier__c"))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("Region__c") AS "Region__c",
    TRIM("Industry") AS "Industry",
    TRIM("Website") AS "Website",
    TRIM("BillingCity") AS "BillingCity",
    TRIM("BillingCountry") AS "BillingCountry",
    TRIM("Legacy_Customer_ID__c") AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Account') }}