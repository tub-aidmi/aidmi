{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region__c) AS "Region__c",
    TRIM(industry) AS "Industry",
    TRIM(website) AS "Website",
    TRIM(billingcity) AS "BillingCity",
    TRIM(billingcountry) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}
