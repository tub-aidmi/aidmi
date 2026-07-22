{{ config(materialized='table') }}

SELECT
    MD5("kundennummer") AS "Id",
    COALESCE(NULLIF(TRIM("unternehmensname"), ''), 'Unknown') AS "Name",
    NULLIF(TRIM("erp_nr"), '') AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM("kundenklasse")) IN ('gold', 'silver', 'bronze') THEN INITCAP(LOWER(TRIM("kundenklasse")))
        WHEN LOWER(TRIM("kundenklasse")) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM("vertriebsgebiet"), '') AS "Region__c",
    CASE 
        WHEN LOWER(TRIM("industrie")) IN ('gesundheitswesen', 'healthcare') THEN 'Healthcare'
        WHEN LOWER(TRIM("industrie")) IN ('it', 'technology') THEN 'Technology'
        WHEN LOWER(TRIM("industrie")) IN ('manufacturing', 'industrie') THEN 'Manufacturing'
        WHEN LOWER(TRIM("industrie")) = 'finanzen' THEN 'Finance'
        ELSE NULLIF(TRIM("industrie"), '')
    END AS "Industry",
    NULLIF(TRIM("homepage"), '') AS "Website",
    NULLIF(TRIM("stadt"), '') AS "BillingCity",
    NULLIF(TRIM("land_region"), '') AS "BillingCountry",
    "kundennummer" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}