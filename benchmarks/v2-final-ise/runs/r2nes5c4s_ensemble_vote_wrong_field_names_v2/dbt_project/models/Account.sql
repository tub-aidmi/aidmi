{{ config(materialized='table') }}

SELECT
    CAST(kunden_nr AS TEXT) AS "Id",
    INITCAP(TRIM(firmenname)) AS "Name",
    CAST(erp_nummer AS TEXT) AS "ERP_Number__c",
    kategorie AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    CAST(kunden_nr AS TEXT) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}