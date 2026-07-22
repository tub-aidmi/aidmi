{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
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
    NULL::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }} AS account
