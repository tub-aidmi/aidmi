{{ config(materialized='table') }}

SELECT
    TRIM(UPPER("id")) AS "Id",
    COALESCE(INITCAP(TRIM("name")), 'Unknown') AS "Name",
    "erp_number__c" AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM("customer_tier__c")) IN ('gold', 'silver', 'bronze', 'platinum') 
        THEN INITCAP(TRIM("customer_tier__c")) 
        ELSE NULL 
    END AS "Customer_Tier__c",
    "region__c" AS "Region__c",
    "industry" AS "Industry",
    "website" AS "Website",
    INITCAP(TRIM("billingcity")) AS "BillingCity",
    INITCAP(TRIM("billingcountry")) AS "BillingCountry",
    TRIM(UPPER("legacy_customer_id__c")) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}