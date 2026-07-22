{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(src.name, '') AS "Name",
    src.erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(src.customer_tier__c) = 'gold' THEN 'Gold'
        WHEN LOWER(src.customer_tier__c) = 'silver' THEN 'Silver'
        WHEN LOWER(src.customer_tier__c) = 'bronze' THEN 'Bronze'
        WHEN LOWER(src.customer_tier__c) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    src.region__c AS "Region__c",
    src.industry AS "Industry",
    src.website AS "Website",
    src.billingcity AS "BillingCity",
    src.billingcountry AS "BillingCountry",
    src.legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS src
