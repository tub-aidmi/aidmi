
{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", '') AS "Name",
    "ERP_Number__c" AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM("Customer_Tier__c")) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM("Customer_Tier__c")) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM("Customer_Tier__c")) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM("Customer_Tier__c")) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    "Region__c" AS "Region__c",
    "Industry" AS "Industry",
    "Website" AS "Website",
    "BillingCity" AS "BillingCity",
    "BillingCountry" AS "BillingCountry",
    "Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Account') }}
