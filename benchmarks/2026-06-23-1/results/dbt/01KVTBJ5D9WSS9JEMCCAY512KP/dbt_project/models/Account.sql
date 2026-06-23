{{ config(materialized='table') }}

SELECT
    kundennummer AS Id,
    unternehmensname AS Name,
    erp_nr AS ERP_Number__c,
    CASE
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'silver', 'bronze', 'platinum') THEN INITCAP(LOWER(TRIM(kundenklasse)))
        ELSE 'Bronze'
    END AS Customer_Tier__c,
    land_region AS Region__c,
    industrie AS Industry,
    homepage AS Website,
    stadt AS BillingCity,
    NULL AS BillingCountry,
    NULL AS Legacy_Customer_ID__c,
    NULL AS CreatedDate,
    NULL AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_master_src', 'master_kunden') }}