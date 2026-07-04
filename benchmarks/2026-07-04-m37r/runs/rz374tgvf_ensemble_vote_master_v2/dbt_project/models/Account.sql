{{ config(materialized='table') }}

SELECT
    MD5(kunden.kundennummer) AS "Id",
    COALESCE(TRIM(kunden.unternehmensname), 'N/A') AS "Name",
    NULLIF(TRIM(kunden.erp_nr), '') AS "ERP_Number__c",
    CASE
        WHEN LOWER(kunden.kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(kunden.kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(kunden.kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(kunden.kundenklasse) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(kunden.vertriebsgebiet), '') AS "Region__c",
    NULLIF(TRIM(kunden.industrie), '') AS "Industry",
    NULLIF(TRIM(kunden.homepage), '') AS "Website",
    NULLIF(TRIM(kunden.stadt), '') AS "BillingCity",
    NULLIF(TRIM(kunden.land_region), '') AS "BillingCountry",
    kunden.kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
