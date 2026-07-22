{{ config(materialized='table') }}

SELECT 
    UPPER(TRIM(id)) AS "Id",
    CASE WHEN TRIM(name) IS NULL OR TRIM(name) = '' THEN 'Unknown' ELSE INITCAP(TRIM(name)) END AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'silver', 'bronze', 'platinum') 
        THEN INITCAP(TRIM(customer_tier__c)) 
        ELSE NULL 
    END AS "Customer_Tier__c",
    TRIM(LOWER(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    TRIM(website) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    INITCAP(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}