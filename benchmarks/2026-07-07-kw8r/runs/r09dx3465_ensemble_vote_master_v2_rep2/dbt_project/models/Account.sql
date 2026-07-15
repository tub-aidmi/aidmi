{{ config(materialized='table') }}

SELECT
    LOWER('acc_' || TRIM(kundennummer)) AS "Id",
    INITCAP(TRIM(unternehmensname)) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kundenklasse)) IN ('A', 'TOP', 'PLATINUM') THEN 'Platinum'
        WHEN UPPER(TRIM(kundenklasse)) IN ('B', 'GOLD') THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('C', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('D', 'BRONZE', 'STD', '') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    UPPER(TRIM(kundennummer)) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
WHERE TRIM(kundennummer) != '' AND kundennummer IS NOT NULL