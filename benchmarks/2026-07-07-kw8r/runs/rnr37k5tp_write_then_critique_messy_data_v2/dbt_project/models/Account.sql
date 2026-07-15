{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    CASE 
        WHEN name IS NULL OR TRIM(name) = '' THEN 'Unknown Account'
        ELSE INITCAP(TRIM(name))
    END AS "Name",
    CAST(erp_number__c AS TEXT) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    CAST(region__c AS TEXT) AS "Region__c",
    CAST(industry AS TEXT) AS "Industry",
    CAST(website AS TEXT) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    CAST(billingcountry AS TEXT) AS "BillingCountry",
    CAST(legacy_customer_id__c AS TEXT) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}