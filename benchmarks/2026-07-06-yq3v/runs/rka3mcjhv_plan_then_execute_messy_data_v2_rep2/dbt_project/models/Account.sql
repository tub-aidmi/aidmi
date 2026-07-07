{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(INITCAP(name)), 'Unknown Account') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN TRIM(UPPER(customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN TRIM(UPPER(customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN TRIM(UPPER(customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN TRIM(UPPER(customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region__c) AS "Region__c",
    TRIM(INITCAP(industry)) AS "Industry",
    TRIM(LOWER(website)) AS "Website",
    TRIM(INITCAP(billingcity)) AS "BillingCity",
    TRIM(INITCAP(billingcountry)) AS "BillingCountry",
    COALESCE(legacy_customer_id__c, id) AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}
