{{ config(materialized='table') }}

SELECT
    "kundennummer" AS "Id",
    COALESCE(NULLIF(TRIM("unternehmensname"), ''), 'Unknown') AS "Name",
    NULLIF(TRIM("erp_nr"), '') AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM("kundenklasse")) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM("kundenklasse")) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM("kundenklasse")) IN ('BRONZE', 'BROWN') THEN 'Bronze'
        WHEN UPPER(TRIM("kundenklasse")) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM("vertriebsgebiet"), '') AS "Region__c",
    CASE 
        WHEN UPPER(TRIM("industrie")) IN ('GESUNDHEITSWESEN', 'HEALTHCARE') THEN 'Healthcare'
        WHEN UPPER(TRIM("industrie")) IN ('INDUSTRIE', 'MANUFACTURING') THEN 'Manufacturing'
        WHEN UPPER(TRIM("industrie")) IN ('IT', 'TECHNOLOGY') THEN 'Technology'
        WHEN UPPER(TRIM("industrie")) = 'FINANZEN' THEN 'Finance'
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