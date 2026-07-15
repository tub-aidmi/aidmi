{{ config(materialized='table') }}

SELECT
    '001' || LOWER(REGEXP_REPLACE(kundennummer, '[^a-z0-9]', '', 'g')) AS "Id",
    COALESCE(unternehmensname, 'Unknown Account') AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD') THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE') THEN 'Bronze'
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATIN', 'PLATINUM') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    INITCAP(LOWER(industrie)) AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}