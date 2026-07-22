{{ config(materialized='table') }}

SELECT
    CAST(k.kundennummer AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(k.unternehmensname), ''), 'Unknown Customer') AS "Name",
    k.erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(k.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(k.kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(k.kundenklasse)) IN ('bronze',) THEN 'Bronze'
        WHEN LOWER(TRIM(k.kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(k.vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(k.industrie)) AS "Industry",
    CASE WHEN k.homepage IS NOT NULL AND TRIM(k.homepage) != '' THEN k.homepage ELSE NULL END AS "Website",
    INITCAP(TRIM(k.stadt)) AS "BillingCity",
    INITCAP(TRIM(k.land_region)) AS "BillingCountry",
    CAST(k.kundennummer AS TEXT) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k