{{ config(materialized='table') }}

SELECT
    GEN_RANDOM_UUID()::TEXT AS "Id",
    COALESCE(NULLIF(TRIM("firmenname"), ''), "kunden_nr") AS "Name",
    TRIM("erp_nummer") AS "ERP_Number__c",
    CASE 
        WHEN TRIM("kategorie") = 'Gold' THEN 'Gold'
        WHEN TRIM("kategorie") = 'Silber' THEN 'Silver'
        WHEN TRIM("kategorie") = 'Bronze' THEN 'Bronze'
        WHEN TRIM("kategorie") = 'Platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("gebiet") AS "Region__c",
    TRIM("branche") AS "Industry",
    TRIM("webseite") AS "Website",
    TRIM("ort") AS "BillingCity",
    TRIM("land") AS "BillingCountry",
    "kunden_nr" AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}