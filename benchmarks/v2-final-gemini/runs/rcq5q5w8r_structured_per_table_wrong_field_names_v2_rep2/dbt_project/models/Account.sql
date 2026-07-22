-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    kunden_nr AS "Id",
    COALESCE(firmenname, 'Unknown Account Name') AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE
        WHEN kategorie = 'Gold' THEN 'Gold'
        WHEN kategorie = 'Silver' THEN 'Silver'
        WHEN kategorie = 'Bronze' THEN 'Bronze'
        WHEN kategorie = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}