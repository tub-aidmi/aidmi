{{ config(materialized='table') }}

SELECT
    'A00' || UPPER(TRIM(kundennummer)) AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown Account') AS "Name",
    CASE WHEN TRIM(erp_nr) = '' THEN NULL ELSE TRIM(erp_nr) END AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'platinum') THEN INITCAP(TRIM(kundenklasse))
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze', 'bronce') THEN 'Bronze'
        ELSE 'Bronze'
    END AS "Customer_Tier__c",
    CASE WHEN TRIM(vertriebsgebiet) = '' THEN NULL ELSE TRIM(vertriebsgebiet) END AS "Region__c",
    CASE WHEN TRIM(industrie) = '' THEN NULL ELSE INITCAP(TRIM(industrie)) END AS "Industry",
    CASE 
        WHEN TRIM(homepage) IS NULL OR TRIM(homepage) = '' THEN NULL
        WHEN LOWER(TRIM(homepage)) NOT LIKE 'http%' THEN 'https://' || LOWER(TRIM(homepage))
        ELSE LOWER(TRIM(homepage))
    END AS "Website",
    CASE WHEN TRIM(stadt) = '' THEN NULL ELSE INITCAP(TRIM(stadt)) END AS "BillingCity",
    CASE WHEN TRIM(land_region) = '' THEN NULL ELSE INITCAP(TRIM(land_region)) END AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}