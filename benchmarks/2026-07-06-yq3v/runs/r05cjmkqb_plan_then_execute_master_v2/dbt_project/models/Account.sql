{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kunden.kundennummer)) AS "Id",
    TRIM(kunden.unternehmensname) AS "Name",
    TRIM(kunden.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN TRIM(UPPER(kunden.kundenklasse)) = 'GOLD' THEN 'Gold'
        WHEN TRIM(UPPER(kunden.kundenklasse)) = 'SILBER' THEN 'Silver'
        WHEN TRIM(UPPER(kunden.kundenklasse)) = 'BRONZE' THEN 'Bronze'
        WHEN TRIM(UPPER(kunden.kundenklasse)) = 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.vertriebsgebiet) AS "Region__c",
    TRIM(kunden.industrie) AS "Industry",
    TRIM(kunden.homepage) AS "Website",
    TRIM(INITCAP(kunden.stadt)) AS "BillingCity",
    TRIM(kunden.land_region) AS "BillingCountry",
    TRIM(kunden.kundennummer) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
