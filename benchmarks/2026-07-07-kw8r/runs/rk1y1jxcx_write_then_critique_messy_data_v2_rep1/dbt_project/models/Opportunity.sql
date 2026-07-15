{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(name, 'Unknown'))) AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'platinum') THEN 'Platinum'
        WHEN LOWER(TRIM(customer_tier__c)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) = 'bronze' THEN 'Bronze'
        ELSE INITCAP(TRIM(customer_tier__c))
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    TRIM(website) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    INITCAP(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(id) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}
WHERE id IS NOT NULL AND TRIM(id) != ''