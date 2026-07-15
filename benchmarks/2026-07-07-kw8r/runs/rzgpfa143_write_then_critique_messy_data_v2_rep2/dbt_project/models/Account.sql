{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE(NULLIF(TRIM("name"), ''), 'Unknown') AS "Name",
    TRIM("erp_number__c") AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER("customer_tier__c")) IN ('gold', 'gld') THEN 'Gold'
        WHEN TRIM(LOWER("customer_tier__c")) IN ('silver', 'slv') THEN 'Silver'
        WHEN TRIM(LOWER("customer_tier__c")) IN ('bronze', 'brnz') THEN 'Bronze'
        WHEN TRIM(LOWER("customer_tier__c")) IN ('platinum', 'plat') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("region__c") AS "Region__c",
    TRIM("industry") AS "Industry",
    TRIM("website") AS "Website",
    TRIM("billingcity") AS "BillingCity",
    TRIM("billingcountry") AS "BillingCountry",
    TRIM("legacy_customer_id__c") AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}