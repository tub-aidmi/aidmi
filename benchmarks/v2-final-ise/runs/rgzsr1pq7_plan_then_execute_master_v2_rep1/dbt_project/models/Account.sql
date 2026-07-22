{{ config(materialized='table') }}

SELECT
    LOWER('acc_' || REPLACE(kundennummer, '-', '_')) AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), 'Unknown') AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}