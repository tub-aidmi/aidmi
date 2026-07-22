{{ config(materialized='table') }}

SELECT
    MD5(kundennummer) AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown') AS "Name",
    NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) IN ('platinum', 'platinen', 'platin') THEN 'Platinum'
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', 'gld') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', 'silber', 'slv') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze', 'bronze', 'brnz') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    NULLIF(TRIM(industrie), '') AS "Industry",
    NULLIF(TRIM(homepage), '') AS "Website",
    NULLIF(TRIM(stadt), '') AS "BillingCity",
    NULLIF(TRIM(land_region), '') AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
