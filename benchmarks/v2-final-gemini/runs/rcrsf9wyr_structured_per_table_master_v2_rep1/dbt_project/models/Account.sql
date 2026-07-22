{{ config(materialized='table') }}

SELECT
    kunden.kundennummer AS "Id",
    COALESCE(TRIM(kunden.unternehmensname), kunden.kundennummer) AS "Name",
    kunden.erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(kunden.kundenklasse) IN ('gold') THEN 'Gold'
        WHEN LOWER(kunden.kundenklasse) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(kunden.kundenklasse) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(kunden.kundenklasse) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    kunden.vertriebsgebiet AS "Region__c",
    kunden.industrie AS "Industry",
    kunden.homepage AS "Website",
    kunden.stadt AS "BillingCity",
    kunden.land_region AS "BillingCountry",
    kunden.kundennummer AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate", -- No direct source for CreatedDate
    NULL::text AS "LastModifiedDate", -- No direct source for LastModifiedDate
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
