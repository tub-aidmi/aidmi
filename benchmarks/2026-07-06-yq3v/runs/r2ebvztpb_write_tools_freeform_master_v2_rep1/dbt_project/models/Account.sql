-- models/Account.sql
{{ config(materialized='table') }}

SELECT
    MD5(kunden.kundennummer) AS "Id",
    COALESCE(kunden.unternehmensname, kunden.kundennummer) AS "Name",
    kunden.erp_nr AS "ERP_Number__c",
    CASE
        WHEN kunden.kundenklasse = 'Gold' THEN 'Gold'
        WHEN kunden.kundenklasse = 'Silver' THEN 'Silver'
        WHEN kunden.kundenklasse = 'Bronze' THEN 'Bronze'
        WHEN kunden.kundenklasse = 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    kunden.vertriebsgebiet AS "Region__c",
    kunden.industrie AS "Industry",
    kunden.homepage AS "Website",
    kunden.stadt AS "BillingCity",
    kunden.land_region AS "BillingCountry",
    kunden.kundennummer AS "Legacy_Customer_ID__c",
    NOW()::text AS "CreatedDate",
    NOW()::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
