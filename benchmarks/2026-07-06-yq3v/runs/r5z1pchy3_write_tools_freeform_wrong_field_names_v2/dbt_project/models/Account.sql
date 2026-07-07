-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    kunden_nr AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE
        WHEN lower(kategorie) = 'gold' THEN 'Gold'
        WHEN lower(kategorie) = 'silver' THEN 'Silver'
        WHEN lower(kategorie) = 'bronze' THEN 'Bronze'
        WHEN lower(kategorie) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
