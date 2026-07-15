{{ config(materialized='table') }}

SELECT
    kunden_nr AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    CAST(erp_nummer AS TEXT) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kategorie)) IN ('gold', 'platinum', 'platin') THEN INITCAP(TRIM(kategorie))
        WHEN LOWER(TRIM(kategorie)) = 'silber' THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) IN ('bronze', 'basic', 'standard') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    webseite AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    land AS "BillingCountry",
    CAST(kunden_nr AS TEXT) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
