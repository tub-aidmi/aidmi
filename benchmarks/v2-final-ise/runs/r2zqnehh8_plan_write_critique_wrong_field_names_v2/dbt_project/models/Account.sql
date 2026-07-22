{{ config(materialized='table') }}

SELECT
    CASE 
        WHEN "kunden_nr" LIKE 'K%' 
        THEN '001' || SUBSTRING("kunden_nr" FROM 2)
        ELSE '001' || LEFT(MD5(CAST("kunden_nr" AS TEXT)), 15)
    END AS "Id",
    COALESCE(INITCAP(TRIM("firmenname")), 'Unknown') AS "Name",
    TRIM("erp_nummer") AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM("kategorie")) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM("kategorie")) = 'SILBER' THEN 'Silver'
        WHEN UPPER(TRIM("kategorie")) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM("kategorie")) = 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("gebiet") AS "Region__c",
    INITCAP("branche") AS "Industry",
    "webseite" AS "Website",
    INITCAP(TRIM("ort")) AS "BillingCity",
    UPPER(TRIM("land")) AS "BillingCountry",
    TRIM("kunden_nr") AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}