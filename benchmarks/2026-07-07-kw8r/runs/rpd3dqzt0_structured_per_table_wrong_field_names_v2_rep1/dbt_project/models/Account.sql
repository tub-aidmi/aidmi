{{ config(materialized='table') }}

SELECT
    '001' || MD5(kunden_nr) AS "Id",
    TRIM(firmenname) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER(kategorie)) = 'premium' THEN 'Platinum'
        WHEN TRIM(LOWER(kategorie)) = 'gold' THEN 'Gold'
        WHEN TRIM(LOWER(kategorie)) IN ('silber', 'silver') THEN 'Silver'
        WHEN TRIM(LOWER(kategorie)) = 'bronze' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    TRIM(branche) AS "Industry",
    CASE 
        WHEN webseite IS NULL OR TRIM(webseite) = '' THEN NULL
        WHEN LOWER(TRIM(webseite)) LIKE 'http://%' OR LOWER(TRIM(webseite)) LIKE 'https://%' THEN TRIM(webseite)
        ELSE 'https://' || TRIM(webseite)
    END AS "Website",
    TRIM(ort) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}