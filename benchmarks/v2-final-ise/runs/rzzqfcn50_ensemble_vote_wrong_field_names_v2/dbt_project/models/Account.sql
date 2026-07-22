{{ config(materialized='table') }}

SELECT
    SUBSTRING(MD5(kunden_nr), 1, 15) AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    INITCAP(kategorie) AS "Customer_Tier__c",
    INITCAP(gebiet) AS "Region__c",
    INITCAP(branche) AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}