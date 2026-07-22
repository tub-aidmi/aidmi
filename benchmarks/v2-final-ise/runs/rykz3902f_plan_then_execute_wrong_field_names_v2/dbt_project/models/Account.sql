{{ config(materialized='table') }}

SELECT
    '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kategorie)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(TRIM(kategorie))
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    TRIM(branche) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    INITCAP(TRIM(land)) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}