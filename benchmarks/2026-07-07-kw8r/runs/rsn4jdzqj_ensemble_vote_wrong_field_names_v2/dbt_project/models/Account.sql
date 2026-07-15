{{ config(materialized='table') }}

SELECT
    "kunden_nr" AS "Id",
    "firmenname" AS "Name",
    "erp_nummer" AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM("kategorie")) = 'PLATIN' THEN 'Platinum'
        WHEN UPPER(TRIM("kategorie")) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM("kategorie")) = 'SILBER' THEN 'Silver'
        WHEN UPPER(TRIM("kategorie")) = 'BRONZE' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("gebiet") AS "Region__c",
    TRIM("branche") AS "Industry",
    TRIM("webseite") AS "Website",
    TRIM("ort") AS "BillingCity",
    TRIM("land") AS "BillingCountry",
    "kunden_nr" AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}