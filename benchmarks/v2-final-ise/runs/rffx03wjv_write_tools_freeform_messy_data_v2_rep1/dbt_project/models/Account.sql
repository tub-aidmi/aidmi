{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(customer_tier__c)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(customer_tier__c)) IN ('GOLD', 'SILBER') THEN 'Gold'
        WHEN UPPER(TRIM(customer_tier__c)) IN ('SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(customer_tier__c)) IN ('BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}
