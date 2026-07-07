{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Account Name') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE UPPER(TRIM(customer_tier__c))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region__c) AS "Region__c",
    TRIM(industry) AS "Industry",
    TRIM(website) AS "Website",
    TRIM(billingcity) AS "BillingCity",
    TRIM(billingcountry) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}
