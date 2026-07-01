{{ config(materialized='table') }}

SELECT
    -- Primary key: pass through as-is (format CUST-XXXX)
    CAST("Id" AS text) AS "Id",

    -- Name is NOT NULL in target; default to 'Unknown' when missing or empty
    CASE
        WHEN TRIM("Name") IS NULL OR TRIM("Name") = '' THEN 'Unknown'
        ELSE INITCAP(TRIM("Name"))
    END AS "Name",

    "ERP_Number__c" AS "ERP_Number__c",

    -- Normalize Customer_Tier__c to enum domain (Gold, Silver, Bronze, Platinum)
    CASE UPPER(TRIM("Customer_Tier__c"))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'BRONZE' THEN 'Bronze'
        ELSE NULL  -- fallback for unmapped values
    END AS "Customer_Tier__c",

    -- Region__c: convert empty strings to NULL
    CASE WHEN TRIM("Region__c") = '' THEN NULL ELSE TRIM("Region__c") END AS "Region__c",

    "Industry" AS "Industry",

    "Website" AS "Website",

    -- BillingCity: convert empty strings to NULL
    CASE WHEN TRIM("BillingCity") = '' THEN NULL ELSE TRIM("BillingCity") END AS "BillingCity",

    "BillingCountry" AS "BillingCountry",

    "Legacy_Customer_ID__c" AS "Legacy_Customer_ID__c",

    -- Metadata columns not present in source: default to sensible values
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Account') }}