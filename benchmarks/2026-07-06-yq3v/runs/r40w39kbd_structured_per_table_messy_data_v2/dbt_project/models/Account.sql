{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}