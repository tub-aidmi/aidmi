{{ config(materialized='table') }}

SELECT
    "kundennummer" AS "Id",
    TRIM("unternehmensname") AS "Name",
    "erp_nr" AS "ERP_Number__c",
    CASE
        WHEN TRIM("kundenklasse") = 'Platin' THEN 'Platinum'
        WHEN TRIM("kundenklasse") = 'Gold' THEN 'Gold'
        WHEN TRIM("kundenklasse") = 'Silber' THEN 'Silver'
        WHEN TRIM("kundenklasse") = 'Bronze' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(INITCAP("land_region")) AS "Region__c",
    TRIM(INITCAP("industrie")) AS "Industry",
    "homepage" AS "Website",
    TRIM(INITCAP("stadt")) AS "BillingCity",
    TRIM(INITCAP("land_region")) AS "BillingCountry",
    "kundennummer" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}