{{ config(materialized='table') }}

SELECT
    kundennummer AS "Id",
    COALESCE(TRIM(unternehmensname), kundennummer) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate", -- No source for creation date
    NULL::TEXT AS "LastModifiedDate", -- No source for last modified date
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
