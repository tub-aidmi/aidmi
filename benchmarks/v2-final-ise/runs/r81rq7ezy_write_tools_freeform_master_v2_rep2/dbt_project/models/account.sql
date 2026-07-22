{{ config(materialized='table') }}

SELECT
    'A' || SUBSTR(MD5(src."kundennummer"), 1, 14) AS "Id",
    src."unternehmensname" AS "Name",
    src."erp_nr" AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(src."kundenklasse")) = 'PLATINUM' THEN 'Platinum'
        WHEN UPPER(TRIM(src."kundenklasse")) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(src."kundenklasse")) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(src."kundenklasse")) = 'BRONZE' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    src."vertriebsgebiet" AS "Region__c",
    src."industrie" AS "Industry",
    src."homepage" AS "Website",
    src."stadt" AS "BillingCity",
    src."land_region" AS "BillingCountry",
    src."kundennummer" AS "Legacy_Customer_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} src
