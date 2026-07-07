{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(account.name, 'Unknown Account') AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE UPPER(TRIM(account.customer_tier__c))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region__c AS "Region__c",
    account.industry AS "Industry",
    account.website AS "Website",
    account.billingcity AS "BillingCity",
    account.billingcountry AS "BillingCountry",
    COALESCE(account.legacy_customer_id__c, account.id) AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account
