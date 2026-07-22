{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, id) AS "Name", -- Name is NOT NULL, fallback to id
    erp_number__c AS "ERP_Number__c",
    CASE LOWER(customer_tier__c)
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'silber' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    COALESCE(legacy_customer_id__c, id) AS "Legacy_Customer_ID__c", -- Using id as the natural key for Legacy_Customer_ID__c if not present
    NULL::TEXT AS "CreatedDate", -- Placeholder
    NULL::TEXT AS "LastModifiedDate", -- Placeholder
    0 AS "IsDeleted" -- Default to 0
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}
