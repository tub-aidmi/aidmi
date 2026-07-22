{{ config(materialized='table') }}

SELECT
    TRIM(UPPER("id")) AS "Id",
    INITCAP(TRIM(COALESCE("name", 'Unknown'))) AS "Name",
    "erp_number__c" AS "ERP_Number__c",
    CASE LOWER(TRIM("customer_tier__c"))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM("region__c")) AS "Region__c",
    "industry" AS "Industry",
    "website" AS "Website",
    "billingcity" AS "BillingCity",
    "billingcountry" AS "BillingCountry",
    "id" AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}