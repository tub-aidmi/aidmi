{{ config(materialized='table') }}

SELECT
    "Id" AS "Id",
    COALESCE("Name", 'Unknown') AS "Name", -- Target is NOT NULL
    "ERP_Number__c" AS "ERP_Number__c",
    CASE
        WHEN TRIM(LOWER(COALESCE("Customer_Tier__c", ''))) IN ('gold') THEN 'Gold'
        WHEN TRIM(LOWER(COALESCE("Customer_Tier__c", ''))) IN ('silver') THEN 'Silver'
        WHEN TRIM(LOWER(COALESCE("Customer_Tier__c", ''))) IN ('bronze') THEN 'Bronze'
        WHEN TRIM(LOWER(COALESCE("Customer_Tier__c", ''))) IN ('platinum') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    "Region__c" AS "Region__c",
    "Industry" AS "Industry",
    "Website" AS "Website",
    "BillingCity" AS "BillingCity",
    "BillingCountry" AS "BillingCountry",
    "Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate", -- Not in source, default to NULL
    NULL AS "LastModifiedDate", -- Not in source, default to NULL
    0 AS "IsDeleted" -- Not in source, default to 0 (false)
FROM {{ source('fixture_messy_data_src', 'Account') }}
