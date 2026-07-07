-- depends_on: {{ source('fixture_messy_data_v2_src', 'account') }}

{{ config(materialized='table') }}

SELECT
    account.id AS "Id",
    COALESCE(TRIM(account.name), 'Unknown Account') AS "Name",
    account.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(account.customer_tier__c)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    account.region__c AS "Region__c",
    account.industry AS "Industry",
    account.website AS "Website",
    account.billingcity AS "BillingCity",
    account.billingcountry AS "BillingCountry",
    account.id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account