
{{ config(materialized='table') }}

SELECT
    kunden.kundennummer AS "Id",
    COALESCE(TRIM(kunden.unternehmensname), kunden.kundennummer) AS "Name",
    TRIM(kunden.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(kunden.kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(kunden.kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(kunden.kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(kunden.kundenklasse) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.vertriebsgebiet) AS "Region__c",
    TRIM(kunden.industrie) AS "Industry",
    TRIM(kunden.homepage) AS "Website",
    TRIM(kunden.stadt) AS "BillingCity",
    TRIM(kunden.land_region) AS "BillingCountry",
    kunden.kundennummer AS "Legacy_Customer_ID__c",
    CAST(NULL AS text) AS "CreatedDate",
    CAST(NULL AS text) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kunden') }} AS kunden
