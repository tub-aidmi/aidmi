{{ config(materialized='table') }}

SELECT
    -- Generate a deterministic Salesforce-style Account ID (001 prefix + 15 chars from MD5 = 18-char Id)
    '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS "Id",
    -- Company name mapped directly; default to 'Unknown' if NULL
    COALESCE(INITCAP(TRIM(firmenname)), 'Unknown') AS "Name",
    -- ERP number passed through as-is
    TRIM(erp_nummer) AS "ERP_Number__c",
    -- Customer tier: kategorie already matches target enum (Gold, Silver, Bronze, Platinum)
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
    -- Website passed through as-is
    TRIM(webseite) AS "Website",
    -- BillingCity from ort
    INITCAP(TRIM(ort)) AS "BillingCity",
    -- BillingCountry from land
    INITCAP(TRIM(land)) AS "BillingCountry",
    -- Legacy customer ID: the natural key kunden_nr
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    -- No date columns in source; use NULL per guidelines
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}