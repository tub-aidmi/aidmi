{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    CAST(erp_number__c AS TEXT) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(TRIM(customer_tier__c))
        ELSE NULL
    END AS "Customer_Tier__c",
    CAST(region__c AS TEXT) AS "Region__c",
    CAST(industry AS TEXT) AS "Industry",
    CAST(website AS TEXT) AS "Website",
    CAST(billingcity AS TEXT) AS "BillingCity",
    CAST(billingcountry AS TEXT) AS "BillingCountry",
    CAST(id AS TEXT) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}