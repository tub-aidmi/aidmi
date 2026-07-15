{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('silver') THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('platinum') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region__c) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    TRIM(website) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    UPPER(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}
