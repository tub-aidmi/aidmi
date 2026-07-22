
{{ config(materialized='table') }}

SELECT
    TRIM(kunden.kunden_nr) AS "Id",
    COALESCE(TRIM(kunden.firmenname), 'Unknown Account') AS "Name",
    TRIM(kunden.erp_nummer) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kunden.kategorie)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kunden.kategorie)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kunden.kategorie)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kunden.kategorie)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.gebiet) AS "Region__c",
    TRIM(kunden.branche) AS "Industry",
    TRIM(kunden.webseite) AS "Website",
    TRIM(kunden.ort) AS "BillingCity",
    TRIM(kunden.land) AS "BillingCountry",
    TRIM(kunden.kunden_nr) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'kunden') }} AS kunden
