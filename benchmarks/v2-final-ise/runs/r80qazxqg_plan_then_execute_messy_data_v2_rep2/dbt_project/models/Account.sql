{{ config(materialized='table') }}

SELECT 
    id AS "Id",
    COALESCE(TRIM(INITCAP(name)), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(customer_tier__c)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) = 'platinum' THEN 'Platinum'
        ELSE NULL 
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region__c)) AS "Region__c",
    TRIM(industry) AS "Industry",
    TRIM(LOWER(website)) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    INITCAP(TRIM(billingcountry)) AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}