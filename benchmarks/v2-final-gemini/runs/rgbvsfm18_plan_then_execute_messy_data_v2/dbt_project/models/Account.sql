-- dbt model for Account

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
    LOWER(TRIM(account.website)) AS "Website",
    INITCAP(TRIM(account.billingcity)) AS "BillingCity",
    INITCAP(TRIM(account.billingcountry)) AS "BillingCountry",
    COALESCE(TRIM(account.legacy_customer_id__c), TRIM(account.id)) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }} AS account