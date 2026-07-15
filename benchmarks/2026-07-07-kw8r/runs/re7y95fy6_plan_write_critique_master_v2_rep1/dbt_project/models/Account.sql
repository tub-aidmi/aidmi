{{ config(materialized='table') }}

SELECT 
    -- Normalize kundennummer: trim, uppercase, strip leading alphabetic prefixes (e.g. 'K001' -> '001')
    REGEXP_REPLACE(TRIM(UPPER(kundennummer)), '^[A-Z]+', '', 'g') AS "Id",
    
    -- Company name; default to UNSPECIFIED since target is NOT NULL
    INITCAP(TRIM(COALESCE(unternehmensname, 'UNSPECIFIED'))) AS "Name",
    
    -- ERP number (raw text)
    TRIM(erp_nr) AS "ERP_Number__c",
    
    -- Customer tier: map source kundenklasse to target enum (Gold, Silver, Bronze, Platinum)
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM') THEN 'Platinum'
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD')       THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER')     THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE')     THEN 'Bronze'
        ELSE NULL  -- consistent fallback for unmapped/unspecified tiers
    END AS "Customer_Tier__c",
    
    -- Sales region
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    
    -- Industry sector
    INITCAP(TRIM(industrie)) AS "Industry",
    
    -- Website: prepend https:// when scheme is missing; keep as-is otherwise
    CASE 
        WHEN TRIM(homepage) IS NOT NULL AND TRIM(homepage) != '' THEN 
            CASE 
                WHEN LOWER(TRIM(homepage)) LIKE 'http%' THEN TRIM(homepage)
                ELSE 'https://' || TRIM(homepage)
            END
        ELSE NULL
    END AS "Website",
    
    -- Billing city
    INITCAP(TRIM(stadt)) AS "BillingCity",
    
    -- Billing country/region
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    
    -- Raw source natural key for auditability
    kundennummer AS "Legacy_Customer_ID__c",
    
    -- Fixed creation / modification timestamps (text format)
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    
    -- Soft-delete flag: 0 = not deleted
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}