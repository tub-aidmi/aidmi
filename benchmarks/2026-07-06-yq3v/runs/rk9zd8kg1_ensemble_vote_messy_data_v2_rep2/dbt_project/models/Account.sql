{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(account.customer_tier__c) = 'gold' THEN 'Gold'
        WHEN LOWER(account.customer_tier__c) = 'silver' THEN 'Silver'
        WHEN LOWER(account.customer_tier__c) = 'silber' THEN 'Silver' -- Mapping German "Silber" to "Silver"
        WHEN LOWER(account.customer_tier__c) = 'bronze' THEN 'Bronze'
        WHEN LOWER(account.customer_tier__c) = 'platinum' THEN 'Platinum'
        WHEN LOWER(account.customer_tier__c) = 'platin' THEN 'Platinum' -- Mapping German "Platin" to "Platinum"
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region__c AS "Region__c",
    account.industry AS "Industry",
    account.website AS "Website",
    account.billingcity AS "BillingCity",
    account.billingcountry AS "BillingCountry",
    account.legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account
