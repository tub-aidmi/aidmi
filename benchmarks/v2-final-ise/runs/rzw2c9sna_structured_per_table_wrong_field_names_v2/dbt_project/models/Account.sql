{{ config(materialized='table') }}

SELECT
    '001' || REGEXP_REPLACE(kunden_nr, '^CUST-?', '') AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    TRIM(erp_nummer) AS "ERP_Number__c",
    kategorie AS "Customer_Tier__c",
    INITCAP(TRIM(gebiet)) AS "Region__c",
    INITCAP(TRIM(branche)) AS "Industry",
    TRIM(webseite) AS "Website",
    INITCAP(TRIM(ort)) AS "BillingCity",
    INITCAP(TRIM(land)) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}