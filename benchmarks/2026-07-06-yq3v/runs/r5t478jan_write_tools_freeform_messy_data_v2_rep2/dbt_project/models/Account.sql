{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE
        WHEN TRIM(UPPER(customer_tier__c)) IN ('GOLD', 'TIER 1') THEN 'Gold'
        WHEN TRIM(UPPER(customer_tier__c)) IN ('SILVER', 'TIER 2') THEN 'Silver'
        WHEN TRIM(UPPER(customer_tier__c)) IN ('BRONZE', 'TIER 3') THEN 'Bronze'
        WHEN TRIM(UPPER(customer_tier__c)) IN ('PLATINUM', 'TIER 0') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}
