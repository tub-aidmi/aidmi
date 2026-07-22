{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Account Id: transform CUST-XXXX to 001XXXX
    '001' || REGEXP_REPLACE(kunden_nr, '\D', '', 'g') AS "Id",
    -- Company name (not null)
    COALESCE(INITCAP(TRIM(firmenname)), 'Unknown Customer') AS "Name",
    -- ERP number from source
    CAST(erp_nummer AS text) AS "ERP_Number__c",
    -- Customer tier: already matches enum values, normalize case
    INITCAP(TRIM(kategorie)) AS "Customer_Tier__c",
    -- Region / geography
    INITCAP(TRIM(gebiet)) AS "Region__c",
    -- Industry (branche maps to standard Salesforce industries)
    INITCAP(TRIM(branche)) AS "Industry",
    -- Website, cleaned up
    TRIM(webseite) AS "Website",
    -- Billing city
    INITCAP(TRIM(ort)) AS "BillingCity",
    -- Billing country
    INITCAP(TRIM(land)) AS "BillingCountry",
    -- Legacy external customer identifier
    CAST(kunden_nr AS text) AS "Legacy_Customer_ID__c",
    -- Audit fields (no source data available — use NULLs as recommended)
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    -- Soft-delete flag
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}