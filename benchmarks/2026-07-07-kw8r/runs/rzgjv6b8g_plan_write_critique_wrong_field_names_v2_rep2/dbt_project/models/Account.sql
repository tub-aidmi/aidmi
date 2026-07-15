{{ config(materialized='table') }}

SELECT
    '001' || LEFT(UPPER(MD5(REGEXP_REPLACE(kunden_nr, '^[^a-zA-Z0-9]+', '', 'i'))), 15) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kategorie)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kategorie)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(kategorie)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(kategorie)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    TRIM(ort) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}