{{ config(materialized='table') }}

SELECT
    MD5(kundennummer) AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), 'Unknown') AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(LOWER(TRIM(kundenklasse)))
        WHEN UPPER(TRIM(kundenklasse)) = 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}