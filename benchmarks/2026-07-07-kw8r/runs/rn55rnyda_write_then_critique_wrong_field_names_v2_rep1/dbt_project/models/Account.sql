{{ config(materialized='table') }}

SELECT 
    -- Salesforce-style Account ID (001 prefix + legacy customer number, zero-padded to 15 chars)
    '001' || LPAD(TRIM(kunden_nr), 12, '0') AS "Id",
    
    -- Company name: handle NULLs with COALESCE fallback
    COALESCE(INITCAP(TRIM(firmenname)), 'Unknown') AS "Name",
    
    -- ERP Number from source (nullable)
    TRIM(erp_nummer) AS "ERP_Number__c",
    
    -- Customer Tier: map business category to Salesforce-style tier
    CASE 
        WHEN LOWER(TRIM(kategorie)) IN ('gold', 'platinum') THEN INITCAP(TRIM(kategorie))
        WHEN LOWER(TRIM(kategorie)) IN ('großunternehmen', 'enterprise', 'large') THEN 'Platinum'
        WHEN LOWER(TRIM(kategorie)) IN ('kmu', 'smm', 'mittelstand', 'mid-market') THEN 'Silver'
        ELSE 'Bronze'
    END AS "Customer_Tier__c",
    
    -- Region from source gebiet column
    INITCAP(TRIM(gebiet)) AS "Region__c",
    
    -- Industry from source branche column
    INITCAP(TRIM(branche)) AS "Industry",
    
    -- Website (raw as-is, nullable)
    TRIM(webseite) AS "Website",
    
    -- Billing City from source ort column
    INITCAP(TRIM(ort)) AS "BillingCity",
    
    -- Billing Country from source land column  
    INITCAP(TRIM(land)) AS "BillingCountry",
    
    -- Legacy Customer ID: direct mapping from source natural key
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    
    -- System-managed dates (not present in source — use current date)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    
    -- IsDeleted: not present in source — default to 0 (active)
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}