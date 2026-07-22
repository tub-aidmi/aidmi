{{ config(materialized='table') }}

SELECT
    '001' || TRIM(kunden_nr) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kategorie)) IN ('gold') THEN INITCAP(TRIM(kategorie))
        WHEN LOWER(TRIM(kategorie)) IN ('platinum', 'premium', 'vip') THEN 'Platinum'
        WHEN LOWER(TRIM(kategorie)) IN ('silver') THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) IN ('bronze', 'standard', 'normal') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    CASE
        WHEN LOWER(TRIM(webseite)) ~ '^https?://' THEN LOWER(TRIM(webseite))
        WHEN TRIM(webseite) IS NOT NULL AND TRIM(webseite) != '' THEN 'https://' || LOWER(TRIM(webseite))
        ELSE NULL
    END AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    INITCAP(TRIM(land)) AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}