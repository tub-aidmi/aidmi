{{ config(materialized='table') }}

SELECT
    "kundennummer" AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM("unternehmensname")), ''), 'Unknown') AS "Name",
    TRIM("erp_nr") AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM("kundenklasse")) IN ('PLATIN', 'PLATINUM') THEN 'Platinum'
        WHEN UPPER(TRIM("kundenklasse")) IN ('GOLD') THEN 'Gold'
        WHEN UPPER(TRIM("kundenklasse")) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM("kundenklasse")) IN ('BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM("land_region")) AS "Region__c",
    INITCAP(TRIM("industrie")) AS "Industry",
    CASE
        WHEN TRIM("homepage") ~* '^https?://' THEN TRIM("homepage")
        ELSE NULL
    END AS "Website",
    INITCAP(TRIM("stadt")) AS "BillingCity",
    INITCAP(TRIM("land_region")) AS "BillingCountry",
    "kundennummer" AS "Legacy_Customer_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}