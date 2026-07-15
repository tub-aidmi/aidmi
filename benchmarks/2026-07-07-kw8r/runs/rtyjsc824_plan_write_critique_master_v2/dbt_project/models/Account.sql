{{ config(materialized='table') }}
SELECT
    MD5(kundennummer) AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), 'Unknown') AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    INITCAP(TRIM(COALESCE(industrie, ''))) AS "Industry",
    CASE
        WHEN homepage IS NOT NULL AND homepage !~ '^https?://' THEN 'http://' || TRIM(homepage)
        ELSE TRIM(homepage)
    END AS "Website",
    INITCAP(TRIM(COALESCE(stadt, ''))) AS "BillingCity",
    INITCAP(TRIM(COALESCE(land_region, ''))) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    '2023-01-01T00:00:00Z' AS "CreatedDate",
    '2023-01-01T00:00:00Z' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}