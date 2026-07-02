{{ config(materialized='table') }}

SELECT
    CAST(kundennummer AS TEXT) AS "Id",
    INITCAP(TRIM(coalesce(unternehmensname, 'Unknown'))) AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    CASE
        WHEN TRIM(coalesce(vertriebsgebiet, '')) = '' THEN NULL
        ELSE INITCAP(TRIM(vertriebsgebiet))
    END AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    homepage AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_kunden') }}
WHERE coalesce(unternehmensname, '') <> '' OR kundennummer IS NOT NULL