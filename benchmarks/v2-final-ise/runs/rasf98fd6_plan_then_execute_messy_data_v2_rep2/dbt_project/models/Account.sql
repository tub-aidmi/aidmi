{{ config(materialized='table') }}

SELECT 
    INITCAP(TRIM(id)) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(customer_tier__c)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(customer_tier__c)) IN ('SILVER', 'SILBER') THEN 'Silver'
        WHEN UPPER(TRIM(customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        ELSE NULL 
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    TRIM(website) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    INITCAP(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(erp_number__c) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}