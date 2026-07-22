-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    TRIM(account.id) AS "Id",
    COALESCE(TRIM(account.name), 'N/A') AS "Name",
    TRIM(account.erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(account.customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(account.region__c) AS "Region__c",
    TRIM(account.industry) AS "Industry",
    TRIM(account.website) AS "Website",
    TRIM(account.billingcity) AS "BillingCity",
    TRIM(account.billingcountry) AS "BillingCountry",
    TRIM(account.legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account