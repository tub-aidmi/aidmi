{{ config(materialized='table') }}

WITH src AS (
    SELECT
        kunden_nr,
        firmenname,
        erp_nummer,
        kategorie,
        gebiet,
        branche,
        webseite,
        ort,
        land
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    -- Salesforce-style Id: prefix with 001 (Account prefix) + numeric portion of customer number
    '001' || REGEXP_REPLACE(kunden_nr, '^CUST-?', '') AS "Id",
    -- Company name
    INITCAP(TRIM(firmenname)) AS "Name",
    -- ERP number
    TRIM(erp_nummer) AS "ERP_Number__c",
    -- Customer tier (already matches enum: Gold, Silver, Bronze, Platinum)
    kategorie AS "Customer_Tier__c",
    -- Region
    INITCAP(TRIM(gebiet)) AS "Region__c",
    -- Industry/sector
    INITCAP(TRIM(branche)) AS "Industry",
    -- Website URL
    TRIM(webseite) AS "Website",
    -- Billing city
    INITCAP(TRIM(ort)) AS "BillingCity",
    -- Billing country
    INITCAP(TRIM(land)) AS "BillingCountry",
    -- Legacy customer ID for traceability
    kunden_nr AS "Legacy_Customer_ID__c",
    -- Audit fields (not available in source)
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM src;