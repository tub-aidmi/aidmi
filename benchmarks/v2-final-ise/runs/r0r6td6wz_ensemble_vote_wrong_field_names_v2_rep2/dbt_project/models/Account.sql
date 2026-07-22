{{ config(materialized='table') }}

SELECT 
    kunden_nr AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kategorie)) IN ('gold', 'platin', 'platinum') THEN 'Platinum'
        WHEN LOWER(TRIM(kategorie)) = 'silber' THEN 'Silver'
        WHEN LOWER(TRIM(kategorie)) IN ('bronze', 'standard') THEN 'Bronze'
        WHEN LOWER(TRIM(kategorie)) IN ('silver', 'premium') THEN 'Silver'
        ELSE 'Gold'
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
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}