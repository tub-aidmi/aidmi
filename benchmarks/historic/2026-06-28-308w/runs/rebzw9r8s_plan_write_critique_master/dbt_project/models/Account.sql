
{{ config(materialized='table') }}

SELECT
    TRIM(kunden.kundennummer) AS "Id",
    COALESCE(TRIM(kunden.unternehmensname), 'Unknown Account Name') AS "Name",
    TRIM(kunden.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kunden.kundenklasse)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kunden.kundenklasse)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(kunden.kundenklasse)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(kunden.kundenklasse)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.vertriebsgebiet) AS "Region__c",
    TRIM(kunden.industrie) AS "Industry",
    TRIM(kunden.homepage) AS "Website",
    TRIM(kunden.stadt) AS "BillingCity",
    TRIM(kunden.land_region) AS "BillingCountry",
    TRIM(kunden.kundennummer) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate", -- No direct source column, prefer NULL as target is not NOT NULL
    NULL AS "LastModifiedDate", -- No direct source column, prefer NULL as target is not NOT NULL
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kunden') }} AS kunden
