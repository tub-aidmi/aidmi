{{ config(materialized='table') }}

SELECT
    kundennummer AS Id,
    unternehmensname AS Name,
    erp_nr AS ERP_Number__c,
    CASE 
        WHEN UPPER(kundenklasse) IN ('GOLD', 'PLATINUM', 'SILVER', 'BRONZE') THEN 
            INITCAP(LOWER(kundenklasse))
        ELSE 'Bronze'
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
