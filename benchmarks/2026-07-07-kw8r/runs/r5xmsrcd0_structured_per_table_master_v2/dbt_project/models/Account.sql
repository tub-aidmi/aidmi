{{ config(materialized='table') }}

SELECT
    MD5(kundennummer)::text AS "Id",
    TRIM(unternehmensname) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER', 'SILBER') THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}