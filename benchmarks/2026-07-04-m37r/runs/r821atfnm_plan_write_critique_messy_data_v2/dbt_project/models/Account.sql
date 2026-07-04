-- dbt model for Account
{{ config(materialized='table') }}

SELECT
    TRIM(account.id) AS "Id",
    COALESCE(TRIM(account.name), 'Unnamed Account') AS "Name",
    TRIM(account.erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN INITCAP(TRIM(account.customer_tier__c)) = 'Gold' THEN 'Gold'
        WHEN INITCAP(TRIM(account.customer_tier__c)) = 'Silver' THEN 'Silver'
        WHEN INITCAP(TRIM(account.customer_tier__c)) = 'Bronze' THEN 'Bronze'
        WHEN INITCAP(TRIM(account.customer_tier__c)) = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(account.region__c) AS "Region__c",
    TRIM(account.industry) AS "Industry",
    TRIM(account.website) AS "Website",
    TRIM(account.billingcity) AS "BillingCity",
    TRIM(account.billingcountry) AS "BillingCountry",
    COALESCE(TRIM(account.legacy_customer_id__c), TRIM(account.id)) AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS account