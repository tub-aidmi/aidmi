{{ config(materialized='table') }}

WITH account_data AS (
    SELECT
        kunden_nr,
        firmenname,
        erp_nummer,
        kategorie,
        gebiet,
        branche,
        webseite,
        ort,
        land
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
    '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS "Id",
    firmenname AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kategorie)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kategorie)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(kategorie)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(kategorie)) = 'PLATINUM' THEN 'Platinum'
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
FROM account_data
