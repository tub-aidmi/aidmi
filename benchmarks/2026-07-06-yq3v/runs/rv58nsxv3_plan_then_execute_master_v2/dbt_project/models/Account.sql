-- dbt model for Account

{{ config(materialized='table') }}

SELECT
    MD5(mk.kundennummer)::text AS "Id",
    COALESCE(INITCAP(TRIM(mk.unternehmensname)), mk.kundennummer) AS "Name",
    mk.erp_nr AS "ERP_Number__c",
    CASE UPPER(TRIM(mk.kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(mk.vertriebsgebiet) AS "Region__c",
    INITCAP(TRIM(mk.industrie)) AS "Industry",
    mk.homepage AS "Website",
    INITCAP(TRIM(mk.stadt)) AS "BillingCity",
    INITCAP(TRIM(mk.land_region)) AS "BillingCountry",
    mk.kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk