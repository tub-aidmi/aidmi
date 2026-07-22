{{ config(materialized='table') }}

SELECT
    "kundennummer" AS "Id",
    TRIM("unternehmensname") AS "Name",
    TRIM("erp_nr") AS "ERP_Number__c",
    CASE 
        WHEN TRIM(LOWER("kundenklasse")) = 'gold' THEN 'Gold'
        WHEN TRIM(LOWER("kundenklasse")) = 'silber' THEN 'Silver'
        WHEN TRIM(LOWER("kundenklasse")) = 'bronze' THEN 'Bronze'
        WHEN TRIM(LOWER("kundenklasse")) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("land_region") AS "Region__c",
    TRIM(INITCAP("industrie")) AS "Industry",
    TRIM("homepage") AS "Website",
    TRIM(INITCAP("stadt")) AS "BillingCity",
    TRIM(INITCAP("land_region")) AS "BillingCountry",
    "kundennummer" AS "Legacy_Customer_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}