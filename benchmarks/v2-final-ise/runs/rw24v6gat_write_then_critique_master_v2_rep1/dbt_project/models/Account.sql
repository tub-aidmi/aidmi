{{ config(materialized='table') }}

SELECT
    MD5(kundennummer) AS "Id",
    COALESCE(TRIM(unternehmensname), '') AS "Name",
    NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'PLATINUM', 'SILVER', 'BRONZE') THEN INITCAP(LOWER(TRIM(kundenklasse)))
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    NULLIF(TRIM(industrie), '') AS "Industry",
    NULLIF(TRIM(homepage), '') AS "Website",
    NULLIF(TRIM(stadt), '') AS "BillingCity",
    NULLIF(TRIM(land_region), '') AS "BillingCountry",
    NULLIF(TRIM(kundennummer), '') AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}