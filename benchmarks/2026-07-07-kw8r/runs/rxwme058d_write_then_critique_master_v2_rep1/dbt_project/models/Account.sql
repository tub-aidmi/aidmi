{{ config(materialized='table') }}
SELECT
    LOWER(MD5(kundennummer)) || '-0000-0000-0000-000000000000' AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown') AS "Name",
    NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) = 'platin' THEN 'Platinum'
        WHEN LOWER(TRIM(kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) = 'bronze' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    CASE
        WHEN LOWER(TRIM(industrie)) IN ('gesundheitswesen', 'healthcare') THEN 'Healthcare'
        WHEN LOWER(TRIM(industrie)) IN ('finanzen', 'finance') THEN 'Finance'
        WHEN LOWER(TRIM(industrie)) IN ('technologie', 'technology') THEN 'Technology'
        WHEN LOWER(TRIM(industrie)) IN ('industrie', 'manufacturing') THEN 'Manufacturing'
        WHEN LOWER(TRIM(industrie)) = 'it' THEN 'IT'
        ELSE INITCAP(TRIM(industrie))
    END AS "Industry",
    CASE
        WHEN homepage IS NOT NULL AND homepage ~ '^[hH][tT][tT][pP][sS]?://'
        THEN REGEXP_REPLACE(TRIM(homepage), '/+$', '', 'g')
        ELSE NULL
    END AS "Website",
    NULLIF(TRIM(stadt), '') AS "BillingCity",
    NULLIF(TRIM(land_region), '') AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}