{{ config(materialized='table') }}

SELECT 
    'ACCT-' || kundennummer AS "Id",
    INITCAP(TRIM(COALESCE(unternehmensname, 'Unknown Account'))) AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    CASE 
        WHEN vertriebsgebiet IS NOT NULL AND TRIM(vertriebsgebiet) != '' THEN INITCAP(TRIM(vertriebsgebiet))
        ELSE NULL
    END AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    homepage AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_kunden') }}