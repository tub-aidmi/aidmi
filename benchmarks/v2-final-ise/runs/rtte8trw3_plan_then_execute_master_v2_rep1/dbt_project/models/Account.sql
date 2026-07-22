{{ config(materialized='table') }}

SELECT
    INITCAP(TRIM("kundennummer")) AS "Id",
    COALESCE(INITCAP(TRIM("unternehmensname")), 'Unknown') AS "Name",
    "erp_nr" AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM("kundenklasse")) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP("kundenklasse")
        ELSE NULL 
    END AS "Customer_Tier__c",
    "vertriebsgebiet" AS "Region__c",
    "industrie" AS "Industry",
    TRIM("homepage") AS "Website",
    INITCAP(TRIM("stadt")) AS "BillingCity",
    UPPER(TRIM("land_region")) AS "BillingCountry",
    INITCAP(TRIM("kundennummer")) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}