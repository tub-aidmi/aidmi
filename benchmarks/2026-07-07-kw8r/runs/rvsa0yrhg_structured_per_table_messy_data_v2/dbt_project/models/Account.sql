{{ config(materialized='table') }}

SELECT 
    -- Salesforce-style 15-char Account Id derived from legacy CUST-XXXX key
    'A00' || LPAD(SUBSTRING(id FROM '\d+')::INTEGER, 12, '0') AS "Id",
    
    -- Name: NOT NULL fallback for the ~6 NULL rows in source
    COALESCE(TRIM(name), 'Unknown Account') AS "Name",
    
    -- ERP_Number__c: pass-through (format: ERP-XXXXX)
    erp_number__c AS "ERP_Number__c",
    
    -- Customer_Tier__c: normalise German/English variants + casing to enum domain
    CASE LOWER(TRIM(customer_tier__c))
        WHEN 'platin'   THEN 'Platinum'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'gold'     THEN 'Gold'
        WHEN 'bronze'   THEN 'Bronze'
        WHEN 'silber'   THEN 'Silver'
        WHEN 'silver'   THEN 'Silver'
        ELSE NULL
    END AS "Customer_Tier__c",
    
    -- Region__c: pass-through (already clean: Benelux, DACH, Nordics, Southern Europe, UK)
    region__c AS "Region__c",
    
    -- Industry: normalise German labels to English equivalents
    CASE LOWER(TRIM(industry))
        WHEN 'finanzen'            THEN 'Finance'
        WHEN 'gesundheitswesen'    THEN 'Healthcare'
        WHEN 'industrie'           THEN 'Manufacturing'
        WHEN 'technologie'         THEN 'Technology'
        WHEN 'finance'             THEN 'Finance'
        WHEN 'healthcare'          THEN 'Healthcare'
        WHEN 'it'                  THEN 'IT'
        WHEN 'manufacturing'       THEN 'Manufacturing'
        WHEN 'technology'          THEN 'Technology'
        ELSE industry
    END AS "Industry",
    
    -- Website: pass-through
    website AS "Website",
    
    -- BillingCity: pass-through (may contain umlauts - keep for fidelity)
    billingcity AS "BillingCity",
    
    -- BillingCountry: pass-through (German country names)
    billingcountry AS "BillingCountry",
    
    -- Legacy key for row-level verification
    id AS "Legacy_Customer_ID__c",
    
    -- CreatedDate / LastModifiedDate: not present in source → NULL
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    
    -- IsDeleted: default to 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'account') }}