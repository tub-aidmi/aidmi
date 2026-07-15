{{ config(materialized='table') }}

SELECT
    CONCAT('ACC_', "kunden"."kunden_nr") AS "Id",
    TRIM("kunden"."firmenname") AS "Name",
    TRIM("kunden"."erp_nummer") AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM("kunden"."kategorie")) IN ('GOLD', 'PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM("kunden"."kategorie")) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM("kunden"."kategorie")) IN ('BRONZE') THEN 'Bronze'
        WHEN UPPER(TRIM("kunden"."kategorie")) IN ('GOLD') THEN 'Gold'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("kunden"."gebiet") AS "Region__c",
    TRIM("kunden"."branche") AS "Industry",
    TRIM("kunden"."webseite") AS "Website",
    TRIM("kunden"."ort") AS "BillingCity",
    TRIM("kunden"."land") AS "BillingCountry",
    "kunden"."kunden_nr" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS "kunden"