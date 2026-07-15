{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    NULLIF(TRIM(erp_number__c), '') AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'gld') THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('silver', 'slv') THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('bronze', 'brnz') THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) IN ('platinum', 'plat', 'plt') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(region__c), '') AS "Region__c",
    NULLIF(TRIM(industry), '') AS "Industry",
    NULLIF(TRIM(website), '') AS "Website",
    NULLIF(TRIM(billingcity), '') AS "BillingCity",
    NULLIF(TRIM(billingcountry), '') AS "BillingCountry",
    NULLIF(TRIM(legacy_customer_id__c), '') AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}
