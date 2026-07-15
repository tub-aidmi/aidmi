{{ config(materialized='table') }}

SELECT
    CAST(kunden_nr AS TEXT) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    CAST(erp_nummer AS TEXT) AS "ERP_Number__c",
    -- Map kategorie to Customer_Tier enum; already matches (Gold, Silver, Bronze, Platinum), fallback NULL
    CASE
        WHEN LOWER(TRIM(kategorie)) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(LOWER(TRIM(kategorie)))
        ELSE NULL
    END AS "Customer_Tier__c",
    CAST(INITCAP(TRIM(gebiet)) AS TEXT) AS "Region__c",
    CAST(INITCAP(TRIM(branche)) AS TEXT) AS "Industry",
    CAST(webseite AS TEXT) AS "Website",
    CAST(INITCAP(TRIM(ort)) AS TEXT) AS "BillingCity",
    CAST(INITCAP(TRIM(land)) AS TEXT) AS "BillingCountry",
    -- Legacy key for row-level verification
    kunden_nr AS "Legacy_Customer_ID__c",
    -- Derived system fields (no source dates in raw data)
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}