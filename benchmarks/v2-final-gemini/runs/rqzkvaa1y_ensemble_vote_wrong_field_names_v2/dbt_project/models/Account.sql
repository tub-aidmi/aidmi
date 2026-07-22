{{
    config(materialized='table')
}}

SELECT
    kunden.kunden_nr AS "Id",
    COALESCE(kunden.firmenname, 'Unknown Account Name') AS "Name",
    kunden.erp_nummer AS "ERP_Number__c",
    kunden.kategorie AS "Customer_Tier__c",
    kunden.gebiet AS "Region__c",
    kunden.branche AS "Industry",
    kunden.webseite AS "Website",
    kunden.ort AS "BillingCity",
    kunden.land AS "BillingCountry",
    kunden.kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden