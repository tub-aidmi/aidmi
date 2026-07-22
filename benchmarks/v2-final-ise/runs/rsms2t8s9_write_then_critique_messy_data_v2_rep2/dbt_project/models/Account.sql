{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Account') AS "Name",
    TRIM(erp_number__c) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'platinum', 'silver', 'bronze') THEN INITCAP(LOWER(TRIM(customer_tier__c)))
        ELSE NULL
    END AS "Customer_Tier__c",
    UPPER(TRIM(region__c)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    LOWER(TRIM(website)) AS "Website",
    INITCAP(TRIM(billingcity)) AS "BillingCity",
    UPPER(TRIM(billingcountry)) AS "BillingCountry",
    TRIM(legacy_customer_id__c) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}