{{ config(materialized='table') }}

SELECT
    "Id",
    CASE 
        WHEN TRIM("Name") IS NULL OR TRIM("Name") = '' THEN 'Unknown'
        ELSE INITCAP(TRIM("Name"))
    END AS "Name",
    CAST("ERP_Number__c" AS text) AS "ERP_Number__c",
    CASE UPPER(TRIM(COALESCE("Customer_Tier__c", '')))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'PLATIN' THEN 'Platinum'
        WHEN 'BRONZE' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    CASE WHEN TRIM(COALESCE("Region__c", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("Region__c")) 
    END AS "Region__c",
    CASE WHEN TRIM(COALESCE("Industry", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("Industry")) 
    END AS "Industry",
    TRIM(COALESCE("Website", '')) AS "Website",
    CASE WHEN TRIM(COALESCE("BillingCity", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("BillingCity")) 
    END AS "BillingCity",
    CASE WHEN TRIM(COALESCE("BillingCountry", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("BillingCountry")) 
    END AS "BillingCountry",
    CAST("Legacy_Customer_ID__c" AS text) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Account') }}