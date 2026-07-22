{{ config(materialized='table') }}
SELECT
    'ACC-' || TRIM(k.kundennummer) AS "Id",
    INITCAP(TRIM(k.unternehmensname)) AS "Name",
    TRIM(k.erp_nr) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(k.kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(k.kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM(k.kundenklasse)) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(k.kundenklasse)) = 'BRONZE' THEN 'Bronze'
        ELSE NULL 
    END AS "Customer_Tier__c",
    INITCAP(TRIM(k.vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(k.industrie)) AS "Industry",
    TRIM(k.homepage) AS "Website",
    INITCAP(TRIM(k.stadt)) AS "BillingCity",
    INITCAP(TRIM(k.land_region)) AS "BillingCountry",
    TRIM(k.kundennummer) AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k