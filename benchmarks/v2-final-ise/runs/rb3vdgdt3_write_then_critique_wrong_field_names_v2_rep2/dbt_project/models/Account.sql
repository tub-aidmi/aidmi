{{ config(materialized='table') }}
SELECT 
    '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS "Id",
    TRIM(firmenname) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER(kategorie)) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(TRIM(kategorie))
        ELSE NULL 
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    TRIM(branche) AS "Industry",
    TRIM(webseite) AS "Website",
    TRIM(ort) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}