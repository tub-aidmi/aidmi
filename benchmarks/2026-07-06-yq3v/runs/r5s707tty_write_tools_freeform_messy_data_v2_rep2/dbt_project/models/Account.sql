{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Account') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE UPPER(customer_tier__c)
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'account') }}
