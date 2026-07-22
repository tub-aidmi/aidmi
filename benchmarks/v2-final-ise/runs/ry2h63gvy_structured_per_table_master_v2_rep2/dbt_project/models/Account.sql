{{ config(materialized='table') }}
SELECT
    LOWER(SUBSTRING(MD5(k."kundennummer"), 1, 18)) AS "Id",
    k."unternehmensname" AS "Name",
    k."erp_nr" AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(k."vertriebsgebiet")) AS "Region__c",
    INITCAP(TRIM(k."industrie")) AS "Industry",
    TRIM(k."homepage") AS "Website",
    INITCAP(TRIM(k."stadt")) AS "BillingCity",
    UPPER(TRIM(k."land_region")) AS "BillingCountry",
    k."kundennummer" AS "Legacy_Customer_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k