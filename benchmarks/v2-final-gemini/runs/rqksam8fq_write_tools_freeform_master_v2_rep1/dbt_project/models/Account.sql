-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    MD5(kunden.kundennummer) AS "Id",
    kunden.unternehmensname AS "Name",
    kunden.erp_nr AS "ERP_Number__c",
    CASE UPPER(TRIM(kunden.kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
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