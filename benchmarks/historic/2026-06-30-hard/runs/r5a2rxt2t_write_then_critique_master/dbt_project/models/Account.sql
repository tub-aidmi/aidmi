-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    kundennummer AS "Id",
    COALESCE(unternehmensname, kundennummer) AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kundenklasse)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kunden') }}