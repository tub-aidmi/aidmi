-- dbt model for Account

{{ config(materialized='table') }}

SELECT
    kundennummer AS "Id",
    COALESCE(TRIM(master_kunden.unternehmensname), 'Unknown Account') AS "Name",
    TRIM(master_kunden.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(master_kunden.kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(master_kunden.vertriebsgebiet) AS "Region__c",
    TRIM(master_kunden.industrie) AS "Industry",
    TRIM(master_kunden.homepage) AS "Website",
    TRIM(master_kunden.stadt) AS "BillingCity",
    TRIM(master_kunden.land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS master_kunden