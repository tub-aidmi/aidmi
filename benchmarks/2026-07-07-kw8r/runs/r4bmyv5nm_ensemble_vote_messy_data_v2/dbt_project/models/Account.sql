{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(COALESCE(NULLIF(TRIM(name), ''), 'Unknown Account')) AS "Name",
    NULLIF(TRIM(erp_number__c), '') AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(TRIM(customer_tier__c))
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(region__c), '') AS "Region__c",
    NULLIF(TRIM(industry), '') AS "Industry",
    LOWER(NULLIF(TRIM(website), '')) AS "Website",
    INITCAP(NULLIF(TRIM(billingcity), '')) AS "BillingCity",
    INITCAP(NULLIF(TRIM(billingcountry), '')) AS "BillingCountry",
    NULLIF(LOWER(TRIM(legacy_customer_id__c)), '') AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}