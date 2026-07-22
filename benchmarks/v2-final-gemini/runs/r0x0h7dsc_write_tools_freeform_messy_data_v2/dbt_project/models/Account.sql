{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'N/A') AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(account.customer_tier__c) = 'gold' THEN 'Gold'
        WHEN LOWER(account.customer_tier__c) = 'silver' THEN 'Silver'
        WHEN LOWER(account.customer_tier__c) = 'bronze' THEN 'Bronze'
        WHEN LOWER(account.customer_tier__c) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region__c AS "Region__c",
    account.industry AS "Industry",
    account.website AS "Website",
    account.billingcity AS "BillingCity",
    account.billingcountry AS "BillingCountry",
    account.legacy_customer_id__c AS "Legacy_Customer_ID__c",
    '2023-01-01' AS "CreatedDate", -- Default value
    '2023-01-01' AS "LastModifiedDate", -- Default value
    0 AS "IsDeleted" -- Default value
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account
