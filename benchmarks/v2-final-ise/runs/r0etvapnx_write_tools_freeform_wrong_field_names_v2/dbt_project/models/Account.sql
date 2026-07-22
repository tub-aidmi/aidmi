{{ config(materialized='table') }}

SELECT 
    '001' || RIGHT('000000' || REGEXP_REPLACE(kunden_nr, '[^0-9]', '', 'g'), 6) AS "Id",
    INITCAP(firmenname) AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kategorie)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kategorie)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(kategorie)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(kategorie)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(gebiet) AS "Region__c",
    INITCAP(branche) AS "Industry",
    webseite AS "Website",
    INITCAP(ort) AS "BillingCity",
    INITCAP(land) AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
