{{ config(materialized='table') }}

SELECT 
    -- Generate Salesforce-style Account ID (001 prefix + customer number)
    ('001' || TRIM(kunden_nr)) AS "Id",
    
    -- Company Name — satisfies NOT NULL constraint with fallback
    COALESCE(TRIM(firmenname), 'Unknown Customer') AS "Name",
    
    -- ERP Number (cleaned)
    TRIM(erp_nummer) AS "ERP_Number__c",
    
    -- Customer Tier: map German category values to English domain
    CASE LOWER(TRIM(kategorie))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silber' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    
    -- Region (proper casing)
    INITCAP(TRIM(gebiet)) AS "Region__c",
    
    -- Industry (proper casing)
    INITCAP(TRIM(branche)) AS "Industry",
    
    -- Website (trimmed)
    TRIM(webseite) AS "Website",
    
    -- Billing City (proper casing)
    INITCAP(TRIM(ort)) AS "BillingCity",
    
    -- Billing Country (ISO-style upper-case)
    UPPER(TRIM(land)) AS "BillingCountry",
    
    -- Legacy natural key for row-level verification
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    
    -- No source date columns available — NULL is preferred over sentinels
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    
    -- Default: not deleted
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}