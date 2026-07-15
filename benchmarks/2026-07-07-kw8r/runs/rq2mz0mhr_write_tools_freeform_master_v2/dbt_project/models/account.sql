{{ config(materialized='table') }}

SELECT
    CAST(UPPER(TRIM(kundennummer)) AS TEXT) AS "Id",
    INITCAP(TRIM(unternehmensname)) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', '1') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silber', 'silver', '2') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze', '3') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platin', 'platinum', '4') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    UPPER(TRIM(land_region)) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
