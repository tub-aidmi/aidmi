{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Account Id (15-char prefix + padded source key)
    'A' || RIGHT('0000000' || CAST(kunden_nr AS INTEGER), 7) AS "Id",

    -- Company / Account name
    firmenname AS "Name",

    -- ERP number from source
    erp_nummer AS "ERP_Number__c",

    -- Customer tier: map kategorie to Gold/Silver/Bronze/Platinum
    CASE
        WHEN LOWER(TRIM(kategorie)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kategorie)) = 'silber' THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kategorie)) = 'platin' THEN 'Platinum'
        -- Fallback: infer from common category names if needed
        ELSE UPPER(LEFT(kategorie, 4)) -- best-effort capitalised guess; could be NULL
    END AS "Customer_Tier__c",

    -- Region (gebiet)
    INITCAP(TRIM(gebiet)) AS "Region__c",

    -- Industry (branche)
    INITCAP(TRIM(branche)) AS "Industry",

    -- Website
    webseite AS "Website",

    -- Billing City (ort)
    INITCAP(TRIM(ort)) AS "BillingCity",

    -- Billing Country (land)
    UPPER(TRIM(land)) AS "BillingCountry",

    -- Legacy customer ID = raw source natural key
    kunden_nr AS "Legacy_Customer_ID__c",

    -- Audit columns (not present in source; use defaults)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}