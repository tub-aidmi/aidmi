-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unknown Account') AS "Name",
    "ERP_Number__c" AS "ERP_Number__c",
    CASE
        WHEN LOWER("Customer_Tier__c") IN ('gold') THEN 'Gold'
        WHEN LOWER("Customer_Tier__c") IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER("Customer_Tier__c") IN ('bronze') THEN 'Bronze'
        WHEN LOWER("Customer_Tier__c") IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    "Region__c" AS "Region__c",
    "Industry" AS "Industry",
    "Website" AS "Website",
    "BillingCity" AS "BillingCity",
    "BillingCountry" AS "BillingCountry",
    "Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Account') }}
