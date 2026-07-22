-- models/Account.sql
{{ config(materialized='table') }}

SELECT
    TRIM(k.kundennummer) AS "Id",
    COALESCE(TRIM(k.unternehmensname), 'Unnamed Account ' || TRIM(k.kundennummer)) AS "Name",
    TRIM(k.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(k.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(k.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(k.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(k.kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(k.vertriebsgebiet) AS "Region__c",
    TRIM(k.industrie) AS "Industry",
    TRIM(k.homepage) AS "Website",
    TRIM(k.stadt) AS "BillingCity",
    TRIM(k.land_region) AS "BillingCountry",
    TRIM(k.kundennummer) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kunden') }} AS k