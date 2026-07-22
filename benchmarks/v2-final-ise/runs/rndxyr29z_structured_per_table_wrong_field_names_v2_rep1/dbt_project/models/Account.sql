{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

mapped AS (
    SELECT
        -- Generate Salesforce-style Account Id deterministically from kunden_nr
        CONCAT('A0', RIGHT('0000000000' || kunden_nr, 10)) AS "Id",
        
        -- Name is required; fallback to NULL if empty
        TRIM(firmenname) AS "Name",
        
        -- ERP Number from source
        TRIM(erp_nummer) AS "ERP_Number__c",
        
        -- Customer Tier: map kategorie values to Gold, Silver, Bronze, Platinum
        CASE LOWER(TRIM(kategorie))
            WHEN 'gold' THEN 'Gold'
            WHEN 'silber' THEN 'Silver'
            WHEN 'bronze' THEN 'Bronze'
            WHEN 'platin' THEN 'Platinum'
            WHEN 'premium' THEN 'Gold'
            WHEN 'basic' THEN 'Bronze'
            ELSE NULL
        END AS "Customer_Tier__c",
        
        -- Region from gebiet
        TRIM(gebiet) AS "Region__c",
        
        -- Industry from branche
        INITCAP(TRIM(branche)) AS "Industry",
        
        -- Website from webseite
        TRIM(webseite) AS "Website",
        
        -- BillingCity from ort
        INITCAP(TRIM(ort)) AS "BillingCity",
        
        -- BillingCountry from land
        INITCAP(TRIM(land)) AS "BillingCountry",
        
        -- Legacy customer ID is the source natural key
        kunden_nr AS "Legacy_Customer_ID__c",
        
        -- Dates not present in source; use NULL per guidelines (prefer NULL over sentinel dates)
        NULL::TEXT AS "CreatedDate",
        NULL::TEXT AS "LastModifiedDate",
        
        -- Not deleted by default
        0 AS "IsDeleted"
        
    FROM source_data
    WHERE TRIM(firmenname) IS NOT NULL AND TRIM(firmenname) != ''
)

SELECT * FROM mapped;