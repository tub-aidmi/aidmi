{{ config(materialized='table') }}

SELECT 
    CAST(id AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    TRIM(website) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    UPPER(TRIM(billingcountry)) AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}