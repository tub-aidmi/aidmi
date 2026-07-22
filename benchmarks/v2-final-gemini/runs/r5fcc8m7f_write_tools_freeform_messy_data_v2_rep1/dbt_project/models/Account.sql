{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Account') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(customer_tier__c)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) = 'silber' THEN 'Silver'
        WHEN LOWER(TRIM(customer_tier__c)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(customer_tier__c)) = 'platinum' THEN 'Platinum'
        WHEN LOWER(TRIM(customer_tier__c)) = 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region__c AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billingcity AS "BillingCity",
    billingcountry AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source(source_name, source_table) }}
