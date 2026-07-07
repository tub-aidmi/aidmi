{{ config(materialized='table') }}

SELECT
    TRIM(kunden.kundennummer) AS "Id",
    COALESCE(TRIM(kunden.unternehmensname), TRIM(kunden.kundennummer)) AS "Name",
    TRIM(kunden.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kunden.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kunden.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(kunden.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kunden.kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.vertriebsgebiet) AS "Region__c",
    TRIM(kunden.industrie) AS "Industry",
    TRIM(kunden.homepage) AS "Website",
    TRIM(kunden.stadt) AS "BillingCity",
    TRIM(kunden.land_region) AS "BillingCountry",
    TRIM(kunden.kundennummer) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
