-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    TRIM(account.id) AS "Id",
    COALESCE(TRIM(account.name), 'Unknown Account') AS "Name",
    TRIM(account.erp_number__c) AS "ERP_Number__c",
    CASE UPPER(TRIM(account.customer_tier__c))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(account.region__c) AS "Region__c",
    TRIM(account.industry) AS "Industry",
    TRIM(account.website) AS "Website",
    TRIM(account.billingcity) AS "BillingCity",
    TRIM(account.billingcountry) AS "BillingCountry",
    TRIM(account.id) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account