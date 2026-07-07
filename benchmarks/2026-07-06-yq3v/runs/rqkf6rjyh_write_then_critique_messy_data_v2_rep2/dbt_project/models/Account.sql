{{ config(materialized='table') }}

SELECT
    TRIM(src.id) AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Account Name') AS "Name",
    TRIM(src.erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(src.customer_tier__c)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(src.customer_tier__c)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(src.customer_tier__c)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(src.customer_tier__c)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(src.region__c) AS "Region__c",
    TRIM(src.industry) AS "Industry",
    TRIM(src.website) AS "Website",
    TRIM(src.billingcity) AS "BillingCity",
    TRIM(src.billingcountry) AS "BillingCountry",
    TRIM(src.legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }} AS src
