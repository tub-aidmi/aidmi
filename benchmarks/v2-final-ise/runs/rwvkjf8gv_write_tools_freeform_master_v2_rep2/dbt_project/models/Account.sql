{{ config(materialized='table') }}

SELECT
    '001' || REPLACE(kundennummer, 'CUST-M', '') AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown') AS "Name",
    NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER', 'SILBER') THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'BRONZ') THEN 'Bronze'
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    NULLIF(TRIM(industrie), '') AS "Industry",
    NULLIF(TRIM(homepage), '') AS "Website",
    NULLIF(TRIM(stadt), '') AS "BillingCity",
    NULLIF(TRIM(land_region), '') AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
