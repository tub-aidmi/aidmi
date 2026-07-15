{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
parsed AS (
    SELECT
        -- Generate Salesforce-style Account Id by prefixing with 001
        CONCAT('001', kunden_nr) AS "Id",
        
        -- Map fields from source
        TRIM(COALESCE(firmenname, '')) AS "Name",
        TRIM(erp_nummer) AS "ERP_Number__c",
        
        -- Customer Tier mapping: normalize German/English terms to Gold/Silver/Bronze/Platinum
        CASE 
            WHEN UPPER(TRIM(kategorie)) = 'PLATINUM' OR UPPER(TRIM(kategorie)) = 'PLATIN' THEN 'Platinum'
            WHEN UPPER(TRIM(kategorie)) = 'GOLD' THEN 'Gold'
            WHEN UPPER(TRIM(kategorie)) = 'SILVER' OR UPPER(TRIM(kategorie)) = 'SILBER' THEN 'Silver'
            WHEN UPPER(TRIM(kategorie)) = 'BRONZE' THEN 'Bronze'
            ELSE NULL
        END AS "Customer_Tier__c",
        
        TRIM(gebiet) AS "Region__c",
        INITCAP(TRIM(branche)) AS "Industry",
        TRIM(webseite) AS "Website",
        INITCAP(TRIM(ort)) AS "BillingCity",
        UPPER(TRIM(land)) AS "BillingCountry",
        
        -- Legacy key for traceability
        kunden_nr AS "Legacy_Customer_ID__c",
        
        -- Static metadata columns
        '2024-01-01'::TEXT AS "CreatedDate",
        '2024-01-01'::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"

    FROM source
)

SELECT * FROM parsed
