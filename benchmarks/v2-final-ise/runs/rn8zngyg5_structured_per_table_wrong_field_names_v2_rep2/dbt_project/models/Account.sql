{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)
SELECT
    -- Generate Salesforce-style Id from customer number
    LEFT('a00' || LPAD(REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g'), 12, '0'), 15) AS "Id",
    
    -- Name maps directly to firmenname; default to empty string if null (NOT NULL constraint)
    COALESCE(TRIM(firmenname), '') AS "Name",
    
    -- ERP_Number__c from erp_nummer
    CAST(erp_nummer AS TEXT) AS "ERP_Number__c",
    
    -- Customer_Tier__c from kategorie (already matches enum: Gold, Silver, Bronze, Platinum)
    INITCAP(TRIM(kategorie)) AS "Customer_Tier__c",
    
    -- Region__c from gebiet
    TRIM(gebiet) AS "Region__c",
    
    -- Industry from branche
    TRIM(branche) AS "Industry",
    
    -- Website from webseite
    TRIM(webseite) AS "Website",
    
    -- BillingCity from ort
    INITCAP(TRIM(ort)) AS "BillingCity",
    
    -- BillingCountry from land
    INITCAP(TRIM(land)) AS "BillingCountry",
    
    -- Legacy key: raw customer number
    kunden_nr AS "Legacy_Customer_ID__c",
    
    -- CreatedDate / LastModifiedDate not present in source — use a stable default
    '2024-01-01'::TEXT AS "CreatedDate",
    '2024-01-01'::TEXT AS "LastModifiedDate",
    
    -- IsDeleted: 0 = not deleted (no deletion flag in source)
    0 AS "IsDeleted"

FROM source