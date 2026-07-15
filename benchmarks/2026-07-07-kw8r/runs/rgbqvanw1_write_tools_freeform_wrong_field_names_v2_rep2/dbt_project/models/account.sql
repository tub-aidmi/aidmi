{{ config(materialized='table') }}

SELECT
    CONCAT('001', LEFT(MD5(kunden_nr), 17)) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kategorie)) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM(kategorie)) IN ('silber', 'silver') THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kategorie)) IN ('platin', 'platinum') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
