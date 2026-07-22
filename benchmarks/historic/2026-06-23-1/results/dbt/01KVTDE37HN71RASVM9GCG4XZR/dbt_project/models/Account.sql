{{ config(materialized='table') }}

SELECT
    kundennummer AS Id,
    unternehmensname AS Name,
    erp_nr AS ERP_Number__c,
    CASE
        WHEN UPPER(kundenklasse) = 'GOLD' THEN 'Gold'
        WHEN UPPER(kundenklasse) = 'SILVER' THEN 'Silver'
        WHEN UPPER(kundenklasse) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(kundenklasse) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS Customer_Tier__c,
    vertriebsgebiet AS Region__c,
    industrie AS Industry,
    homepage AS Website,
    stadt AS BillingCity,
    land_region AS BillingCountry,
    kundennummer AS Legacy_Customer_ID__c,
    CURRENT_TIMESTAMP::text AS CreatedDate,
    CURRENT_TIMESTAMP::text AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_master_src', 'master_kunden') }}