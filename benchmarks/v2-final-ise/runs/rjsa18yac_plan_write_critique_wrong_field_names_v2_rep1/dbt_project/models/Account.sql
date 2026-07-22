{{ config(materialized='table') }}

SELECT
    '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    CASE 
        WHEN TRIM(kategorie) IN ('Gold', 'Silver', 'Bronze', 'Platinum') THEN TRIM(kategorie)
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(gebiet) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    CASE 
        WHEN TRIM(webseite) ~ '^https?://' THEN TRIM(webseite)
        ELSE 'http://' || TRIM(webseite)
    END AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    TRIM(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}