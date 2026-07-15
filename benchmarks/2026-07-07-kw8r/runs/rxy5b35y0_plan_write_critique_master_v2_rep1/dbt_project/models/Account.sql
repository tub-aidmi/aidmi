{{ config(materialized='table') }}
SELECT
    MD5(kundennummer) AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), INITCAP(TRIM(kundennummer))) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(LOWER(TRIM(kundenklasse)))
        WHEN UPPER(TRIM(kundenklasse)) = 'PLATIN' THEN 'Platinum'
        WHEN UPPER(TRIM(kundenklasse)) = 'SILBER' THEN 'Silver'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(land_region) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}