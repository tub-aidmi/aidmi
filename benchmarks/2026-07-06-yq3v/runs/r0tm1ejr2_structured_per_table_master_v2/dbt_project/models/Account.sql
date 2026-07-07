{{ config(materialized='table') }}

SELECT
    TRIM(kunden.kundennummer) AS "Id",
    COALESCE(TRIM(kunden.unternehmensname), TRIM(kunden.kundennummer)) AS "Name",
    TRIM(kunden.erp_nr) AS "ERP_Number__c",
    CASE UPPER(TRIM(kunden.kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.vertriebsgebiet) AS "Region__c",
    TRIM(kunden.industrie) AS "Industry",
    TRIM(kunden.homepage) AS "Website",
    TRIM(kunden.stadt) AS "BillingCity",
    TRIM(kunden.land_region) AS "BillingCountry",
    TRIM(kunden.kundennummer) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
