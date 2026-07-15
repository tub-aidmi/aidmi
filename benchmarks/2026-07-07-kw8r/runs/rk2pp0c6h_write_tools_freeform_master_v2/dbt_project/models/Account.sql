{{ config(materialized='table') }}

WITH customer_data AS (
    SELECT
        kundennummer,
        unternehmensname,
        erp_nr,
        kundenklasse,
        vertriebsgebiet,
        industrie,
        homepage,
        stadt,
        land_region
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    kundennummer AS "Id",
    COALESCE(unternehmensname, 'Unknown') AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') 
        THEN INITCAP(LOWER(TRIM(kundenklasse)))
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM customer_data
