{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Id: standardized '001' prefix + zero-padded numeric part from kunden_nr
    '001' || LPAD(SUBSTRING(kunden_nr FROM '\d+')::INTEGER::TEXT, 12, '0') AS "Id",
    -- Company name (normalized)
    INITCAP(TRIM(firmenname)) AS "Name",
    -- ERP number (strip 'ERP-' prefix for cleanliness)
    REGEXP_REPLACE(erp_nummer, '^ERP-', '') AS "ERP_Number__c",
    -- Customer tier: map from kategorie into declared enum domain
    CASE
        WHEN UPPER(TRIM(kategorie)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kategorie)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(kategorie)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(kategorie)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    -- Region from gebiet
    INITCAP(TRIM(gebiet)) AS "Region__c",
    -- Industry from branche
    INITCAP(TRIM(branche)) AS "Industry",
    -- Website (trim whitespace)
    TRIM(webseite) AS "Website",
    -- Billing city
    INITCAP(TRIM(ort)) AS "BillingCity",
    -- Billing country
    INITCAP(TRIM(land)) AS "BillingCountry",
    -- Legacy customer id (raw source natural key for row-level verification)
    kunden_nr AS "Legacy_Customer_ID__c",
    -- Date fields not available in source — NULL preferred over sentinel dates
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    -- Default deleted flag
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}