{{ config(materialized='table') }}

SELECT 
    TRIM(kundennummer) AS "Id",
    unternehmensname AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}