{{ config(materialized='table') }}

SELECT 
    MD5(kundennummer) AS "Id",
    TRIM(COALESCE(unternehmensname, '')) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'golden') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silber', 'silver') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(TRIM(kundenklasse)) IN ('platin', 'platinum') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}