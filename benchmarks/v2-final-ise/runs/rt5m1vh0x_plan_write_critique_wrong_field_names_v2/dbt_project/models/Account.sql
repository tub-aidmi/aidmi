{{ config(materialized='table') }}

SELECT
    MD5(kunden_nr) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN TRIM(UPPER(kategorie)) IN ('PLATINUM', 'PREMIUM') THEN 'Platinum'
        WHEN TRIM(UPPER(kategorie)) IN ('GOLD', 'STANDARD') THEN 'Gold'
        WHEN TRIM(UPPER(kategorie)) IN ('SILVER', 'BASIC') THEN 'Silver'
        WHEN TRIM(UPPER(kategorie)) = 'BRONZE' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}