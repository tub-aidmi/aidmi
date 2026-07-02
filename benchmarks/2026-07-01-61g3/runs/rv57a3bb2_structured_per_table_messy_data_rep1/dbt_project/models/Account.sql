{{ config(materialized='table') }}

SELECT
    -- Base columns
    "Id",
    
    -- Name: NOT NULL constraint - use 'Unknown' for missing values
    CASE 
        WHEN TRIM("Name") IS NULL OR TRIM("Name") = '' THEN 'Unknown'
        ELSE INITCAP(TRIM("Name"))
    END AS "Name",
    
    -- ERP_Number__c: text, nullable
    CAST("ERP_Number__c" AS text) AS "ERP_Number__c",
    
    -- Customer_Tier__c: map to enum (Gold, Silver, Bronze, Platinum)
    CASE UPPER(TRIM(COALESCE("Customer_Tier__c", '')))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' OR 'SILBER' THEN 'Silver'  -- German translation SILBER -> Silver
        WHEN 'PLATINUM' OR 'PLATIN' THEN 'Platinum'  -- German translation Platin -> Platinum  
        WHEN 'BRONZE' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    
    -- Region__c: normalize empty strings to NULL
    CASE WHEN TRIM(COALESCE("Region__c", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("Region__c")) 
    END AS "Region__c",
    
    -- Industry: normalize case with INITCAP
    CASE WHEN TRIM(COALESCE("Industry", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("Industry")) 
    END AS "Industry",
    
    -- Website: trim whitespace
    TRIM(COALESCE("Website", '')) AS "Website",
    
    -- BillingCity: normalize case
    CASE WHEN TRIM(COALESCE("BillingCity", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("BillingCity")) 
    END AS "BillingCity",
    
    -- BillingCountry: normalize case with INITCAP
    CASE WHEN TRIM(COALESCE("BillingCountry", '')) = '' THEN NULL 
         ELSE INITCAP(TRIM("BillingCountry")) 
    END AS "BillingCountry",
    
    -- Legacy_Customer_ID__c: same as Id (or keep original)
    CAST("Legacy_Customer_ID__c" AS text) AS "Legacy_Customer_ID__c",
    
    -- Derived columns not in source - default values
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_src', 'Account') }}