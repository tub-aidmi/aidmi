{{ config(materialized='table') }}

SELECT
    '001' || MD5(kundennummer) AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), kundennummer) AS "Name",
    NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silbern') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze', 'bronzen') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum', 'platin', 'platinium') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    CASE 
        WHEN LOWER(TRIM(industrie)) IN ('gesundheitswesen', 'healthcare') THEN 'Healthcare'
        WHEN LOWER(TRIM(industrie)) IN ('finanzen', 'finance') THEN 'Finance'
        WHEN LOWER(TRIM(industrie)) IN ('technologie', 'technology') THEN 'Technology'
        WHEN LOWER(TRIM(industrie)) IN ('it') THEN 'IT'
        WHEN LOWER(TRIM(industrie)) IN ('manufacturing', 'industrie') THEN 'Manufacturing'
        ELSE NULLIF(TRIM(industrie), '')
    END AS "Industry",
    NULLIF(TRIM(homepage), '') AS "Website",
    NULLIF(TRIM(stadt), '') AS "BillingCity",
    NULLIF(TRIM(land_region), '') AS "BillingCountry",
    NULLIF(TRIM(kundennummer), '') AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}