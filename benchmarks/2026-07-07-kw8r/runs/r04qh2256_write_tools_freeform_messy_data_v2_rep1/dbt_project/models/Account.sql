{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
     0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}