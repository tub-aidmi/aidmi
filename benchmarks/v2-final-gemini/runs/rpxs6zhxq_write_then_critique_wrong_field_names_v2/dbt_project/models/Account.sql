-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    MD5(kunden_nr) AS "Id",
    COALESCE(firmenname, 'Unknown') AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE kategorie
        WHEN 'Gold' THEN 'Gold'
        WHEN 'Silver' THEN 'Silver'
        WHEN 'Bronze' THEN 'Bronze'
        WHEN 'Platinum' THEN 'Platinum'
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