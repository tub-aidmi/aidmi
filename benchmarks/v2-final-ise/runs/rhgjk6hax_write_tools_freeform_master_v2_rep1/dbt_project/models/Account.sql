{{ config(materialized='table') }}

SELECT
    '001' || SUBSTRING(MD5(kundennummer) FROM 1 FOR 15) AS "Id",
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown') AS "Name",
    NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(LOWER(TRIM(kundenklasse)))
        ELSE NULL
    END AS "Customer_Tier__c",
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    NULLIF(TRIM(industrie), '') AS "Industry",
    NULLIF(TRIM(homepage), '') AS "Website",
    NULLIF(TRIM(stadt), '') AS "BillingCity",
    NULLIF(TRIM(land_region), '') AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
