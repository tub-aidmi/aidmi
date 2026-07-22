{{ config(materialized='table') }}

SELECT
    '001' || substr(md5(kundennummer), 1, 15) AS "Id",
    CASE WHEN TRIM(unternehmensname) IS NOT NULL AND TRIM(unternehmensname) != '' THEN INITCAP(upper(unternehmensname)) ELSE 'Unknown Customer' END AS "Name",
    CAST(erp_nr AS TEXT) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'PLATINUM') THEN 'Platinum'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'STANDARD', '') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    INITCAP(industrie) AS "Industry",
    homepage AS "Website",
    INITCAP(stadt) AS "BillingCity",
    INITCAP(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
WHERE kundennummer IS NOT NULL