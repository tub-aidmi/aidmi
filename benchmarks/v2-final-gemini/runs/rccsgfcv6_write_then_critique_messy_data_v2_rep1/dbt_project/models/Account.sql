-- dbt model for Account

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Account Name') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE UPPER(TRIM(customer_tier__c))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'PLATIN' THEN 'Platinum' -- Assuming 'Platin' is an alternative for Platinum
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate", -- Not present in source, defaulting to NULL
    NULL AS "LastModifiedDate", -- Not present in source, defaulting to NULL
    0 AS "IsDeleted" -- Not present in source, defaulting to 0 (false)
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}