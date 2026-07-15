{{ config(materialized='table') }}

SELECT
    'A' || MD5(kundennummer) AS "Id",
    INITCAP(TRIM(unternehmensname)) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD') THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'KUPFER') THEN 'Bronze'
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATIN', 'PLATINUM') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}