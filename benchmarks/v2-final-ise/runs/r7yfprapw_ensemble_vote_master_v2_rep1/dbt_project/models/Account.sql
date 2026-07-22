{{ config(materialized='table') }}

SELECT
    MD5(kundennummer || 'Account')::TEXT AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), kundennummer) AS "Name",
    NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(LOWER(TRIM(kundenklasse)))
        WHEN UPPER(TRIM(kundenklasse)) = 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    CASE 
        WHEN UPPER(TRIM(industrie)) IN ('MANUFACTURING', 'IT', 'TECHNOLOGY', 'INDUSTRIE') THEN 'Manufacturing'
        WHEN UPPER(TRIM(industrie)) IN ('GESUNDHEITSWESEN', 'HEALTHCARE') THEN 'Healthcare'
        WHEN UPPER(TRIM(industrie)) = 'FINANZEN' THEN 'Finance'
        ELSE NULLIF(TRIM(industrie), '')
    END AS "Industry",
    NULLIF(TRIM(homepage), '') AS "Website",
    NULLIF(TRIM(stadt), '') AS "BillingCity",
    NULLIF(TRIM(land_region), '') AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}