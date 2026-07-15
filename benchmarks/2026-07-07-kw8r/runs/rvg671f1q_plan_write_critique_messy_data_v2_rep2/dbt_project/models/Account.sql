{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(id)) AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM(name)), ''), 'Unknown') AS "Name",
    erp_number__c AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(customer_tier__c)) IN ('GOLD', 'PLATINUM', 'SILVER', 'BRONZE') THEN INITCAP(TRIM(customer_tier__c))
        ELSE NULL 
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    LOWER(TRIM(website)) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    INITCAP(TRIM(billingcountry)) AS "BillingCountry",
    legacy_customer_id__c AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}