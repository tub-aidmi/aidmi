-- dbt model for Account

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unknown Account') AS "Name",
    "ERP_Number__c" AS "ERP_Number__c",
    CASE
        WHEN LOWER("Customer_Tier__c") = 'gold' THEN 'Gold'
        WHEN LOWER("Customer_Tier__c") = 'silver' OR LOWER("Customer_Tier__c") = 'silber' THEN 'Silver'
        WHEN LOWER("Customer_Tier__c") = 'bronze' THEN 'Bronze'
        WHEN LOWER("Customer_Tier__c") = 'platinum' OR LOWER("Customer_Tier__c") = 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    "Region__c" AS "Region__c",
    "Industry" AS "Industry",
    "Website" AS "Website",
    "BillingCity" AS "BillingCity",
    "BillingCountry" AS "BillingCountry",
    "Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Account') }}