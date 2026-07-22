{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    INITCAP(TRIM(COALESCE(name, 'Unknown'))) AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'platinum', 'bronze') THEN LOWER(TRIM(customer_tier__c))
        WHEN LOWER(TRIM(customer_tier__c)) IN ('silver', 'silber') THEN 'silver'
        ELSE NULL 
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    LOWER(TRIM(website)) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    UPPER(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(COALESCE(legacy_customer_id__c, erp_number__c)) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}