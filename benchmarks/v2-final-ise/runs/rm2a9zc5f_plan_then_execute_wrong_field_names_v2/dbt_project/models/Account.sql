{{ config(materialized='table') }}

SELECT
    -- Generate SFDC-style Account ID: prepend '001', pad to 18 chars for API consistency
    LPAD('001' || UPPER(TRIM(kunden_nr)), 18, '0') AS "Id",

    -- Account Name from firmenname; NULL if empty or blank
    CASE WHEN TRIM(firmenname) = '' THEN NULL ELSE INITCAP(TRIM(firmenname)) END AS "Name",

    -- ERP Number mapped directly with whitespace trimming
    TRIM(erp_nummer) AS "ERP_Number__c",

    -- Customer Tier: normalize German/English variants to target enum; fallback Bronze
    CASE
        WHEN UPPER(TRIM(kategorie)) IN ('GOLD')                    THEN 'Gold'
        WHEN UPPER(TRIM(kategorie)) IN ('SILVER', 'SILBER')       THEN 'Silver'
        WHEN UPPER(TRIMIT(kategorie)) IN ('PLATINUM', 'PLATIN')   THEN 'Platinum'
        ELSE 'Bronze'
    END AS "Customer_Tier__c",

    -- Region mapped directly (German "Gebiet" → English Region)
    INITCAP(TRIM(gebiet)) AS "Region__c",

    -- Industry mapped directly (German "Branche" → English Industry)
    INITCAP(TRIM(branche)) AS "Industry",

    -- Website: prepend https:// if protocol is missing and value is not null/blank
    CASE
        WHEN webseite IS NULL OR TRIM(webseite) = '' THEN NULL
        WHEN UPPER(TRIM(webseite)) LIKE 'HTTP:%'      THEN TRIM(webseite)
        ELSE 'https://' || TRIM(webseite)
    END AS "Website",

    -- Billing City mapped directly (German "Ort")
    INITCAP(TRIM(ort)) AS "BillingCity",

    -- Billing Country mapped directly
    TRIM(land) AS "BillingCountry",

    -- Legacy source key for row-level verification
    kunden_nr AS "Legacy_Customer_ID__c",

    -- Audit columns not present in source — populate with current run values
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0                                             AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}