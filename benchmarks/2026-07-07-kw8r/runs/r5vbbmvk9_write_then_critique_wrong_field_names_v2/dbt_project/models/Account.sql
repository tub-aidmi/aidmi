{{ config(materialized='table') }}

SELECT
    '001' || SUBSTRING(MD5(kunden_nr) FROM 1 FOR 14)::text AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    INITCAP(TRIM(kategorie)) AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    INITCAP(TRIM(land)) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}