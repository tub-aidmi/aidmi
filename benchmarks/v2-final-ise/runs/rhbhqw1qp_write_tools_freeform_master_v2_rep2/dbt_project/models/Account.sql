{{ config(materialized='table') }}

SELECT
    CAST(k.kundennummer AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(k.unternehmensname, ''))) AS "Name",
    k.erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(COALESCE(k.kundenklasse, ''))) IN ('GOLD', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(COALESCE(k.kundenklasse, ''))) = 'SILBER' OR UPPER(TRIM(COALESCE(k.kundenklasse, ''))) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(COALESCE(k.kundenklasse, ''))) = 'BRONZE' OR UPPER(TRIM(COALESCE(k.kundenklasse, ''))) = 'BRONZ' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(k.vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(k.industrie)) AS "Industry",
    TRIM(COALESCE(k.homepage, '')) AS "Website",
    INITCAP(TRIM(k.stadt)) AS "BillingCity",
    INITCAP(TRIM(k.land_region)) AS "BillingCountry",
    k.kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k
