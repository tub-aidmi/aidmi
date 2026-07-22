{{ config(materialized='table') }}

SELECT
    "kunden_nr" AS "Id",
    TRIM("firmenname") AS "Name",
    TRIM("erp_nummer") AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER("kategorie")) IN ('premium', 'platinum') THEN 'Platinum'
        WHEN TRIM(LOWER("kategorie")) IN ('high', 'gold') THEN 'Gold'
        WHEN TRIM(LOWER("kategorie")) IN ('medium', 'silver') THEN 'Silver'
        WHEN TRIM(LOWER("kategorie")) IN ('low', 'bronze') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("gebiet") AS "Region__c",
    TRIM("branche") AS "Industry",
    TRIM("webseite") AS "Website",
    TRIM("ort") AS "BillingCity",
    TRIM(UPPER("land")) AS "BillingCountry",
    "kunden_nr" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}