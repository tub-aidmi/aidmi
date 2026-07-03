{{ config(materialized='table') }}

SELECT
    'A-' || RIGHT(kundennummer, LENGTH(kundennummer) - 4) AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), 'Unknown Customer') AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_kunden') }}