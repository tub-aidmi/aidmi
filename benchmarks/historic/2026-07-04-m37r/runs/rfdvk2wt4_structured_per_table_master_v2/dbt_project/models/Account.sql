{{ config(materialized='table') }}

SELECT
    MD5(kunden.kundennummer) AS "Id",
    COALESCE(kunden.unternehmensname, kunden.kundennummer) AS "Name",
    kunden.erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kunden.kundenklasse)) IN ('platin', 'platinum') THEN 'Platinum'
        WHEN LOWER(TRIM(kunden.kundenklasse)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kunden.kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kunden.kundenklasse)) IN ('gold') THEN 'Gold'
        ELSE NULL
    END AS "Customer_Tier__c",
    kunden.vertriebsgebiet AS "Region__c",
    kunden.industrie AS "Industry",
    kunden.homepage AS "Website",
    kunden.stadt AS "BillingCity",
    kunden.land_region AS "BillingCountry",
    kunden.kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden