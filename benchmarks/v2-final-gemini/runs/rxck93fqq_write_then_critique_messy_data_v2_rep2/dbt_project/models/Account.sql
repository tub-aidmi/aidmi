-- depends_on: {{ source('fixture_messy_data_v2_src', 'account') }}

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Account') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(customer_tier__c) = 'gold' THEN 'Gold'
        WHEN LOWER(customer_tier__c) = 'silver' THEN 'Silver'
        WHEN LOWER(customer_tier__c) = 'bronze' THEN 'Bronze'
        WHEN LOWER(customer_tier__c) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}