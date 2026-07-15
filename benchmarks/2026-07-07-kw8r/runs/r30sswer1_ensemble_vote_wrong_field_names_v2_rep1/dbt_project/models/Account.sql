{{ config(materialized='table') }}

SELECT 
    '001' || LPAD(REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g'), 9, '0') AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    kategorie AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}