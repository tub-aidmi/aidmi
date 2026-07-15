{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    ERP_Number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'gld') THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('silver', 'slv') THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('bronze', 'brnz') THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('platinum', 'plat') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}