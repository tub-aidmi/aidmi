-- dbt model for Account

{{ config(materialized='table') }}

SELECT
    s.id AS "Id",
    COALESCE(TRIM(s.name), 'Unnamed Account') AS "Name",
    s.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(s.customer_tier__c)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(s.customer_tier__c)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(s.customer_tier__c)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(s.customer_tier__c)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    s.region__c AS "Region__c",
    s.industry AS "Industry",
    s.website AS "Website",
    s.billingcity AS "BillingCity",
    s.billingcountry AS "BillingCountry",
    s.legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS s