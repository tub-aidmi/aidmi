{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE(NULLIF(TRIM(INITCAP("name")), ''), 'Unknown') AS "Name",
    TRIM("erp_number__c") AS "ERP_Number__c",
    CASE
        WHEN TRIM(UPPER("customer_tier__c")) IN ('PLATINUM', 'GOLD', 'SILVER', 'BRONZE') THEN INITCAP(LOWER(TRIM("customer_tier__c")))
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(INITCAP("region__c")) AS "Region__c",
    TRIM(INITCAP("industry")) AS "Industry",
    TRIM("website") AS "Website",
    TRIM(INITCAP("billingcity")) AS "BillingCity",
    TRIM(INITCAP("billingcountry")) AS "BillingCountry",
    "legacy_customer_id__c" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}