{{ config(materialized='table') }}

SELECT
    "kundennummer" AS "Id",
    COALESCE("unternehmensname", '') AS "Name",
    "erp_nr" AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM("kundenklasse")) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(LOWER(TRIM("kundenklasse")))
        ELSE NULL
    END AS "Customer_Tier__c",
    "vertriebsgebiet" AS "Region__c",
    CASE
        WHEN UPPER(TRIM("industrie")) = 'GESUNDHEITSWESEN' THEN 'Healthcare'
        WHEN UPPER(TRIM("industrie")) = 'FINANZEN' THEN 'Finance'
        WHEN UPPER(TRIM("industrie")) = 'INDUSTRIE' THEN 'Manufacturing'
        ELSE INITCAP(TRIM("industrie"))
    END AS "Industry",
    "homepage" AS "Website",
    "stadt" AS "BillingCity",
    "land_region" AS "BillingCountry",
    "kundennummer" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}