{{ config(materialized='table') }}

SELECT
    "id" AS "Id",
    COALESCE(NULLIF(TRIM("name"), ''), 'Unknown') AS "Name",
    NULLIF(TRIM("erp_number__c"), '') AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM("customer_tier__c")) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM("customer_tier__c")) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM("customer_tier__c")) IN ('SILVER', 'SILBER') THEN 'Silver'
        WHEN UPPER(TRIM("customer_tier__c")) IN ('BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM("region__c"), '') AS "Region__c",
    CASE
        WHEN UPPER(TRIM("industry")) IN ('IT', 'TECHNOLOGY', 'TECHNOLOGIE') THEN 'IT'
        WHEN UPPER(TRIM("industry")) IN ('HEALTHCARE', 'GESUNDHEITSWESEN') THEN 'Healthcare'
        WHEN UPPER(TRIM("industry")) IN ('FINANCE', 'FINANZEN') THEN 'Finance'
        WHEN UPPER(TRIM("industry")) IN ('MANUFACTURING', 'INDUSTRIE') THEN 'Manufacturing'
        ELSE NULLIF(TRIM("industry"), '')
    END AS "Industry",
    NULLIF(TRIM("website"), '') AS "Website",
    NULLIF(TRIM("billingcity"), '') AS "BillingCity",
    NULLIF(TRIM("billingcountry"), '') AS "BillingCountry",
    NULLIF(TRIM("legacy_customer_id__c"), '') AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'account') }}