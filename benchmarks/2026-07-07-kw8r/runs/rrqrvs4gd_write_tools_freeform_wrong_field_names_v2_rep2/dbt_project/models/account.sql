{{ config(materialized='table') }}

SELECT
    CONCAT('A-', kunden_nr) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kategorie)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kategorie)) = 'silber' THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kategorie)) = 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    INITCAP(TRIM(land)) AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
