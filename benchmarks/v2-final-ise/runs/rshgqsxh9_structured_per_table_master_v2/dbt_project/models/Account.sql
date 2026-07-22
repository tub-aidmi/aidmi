{{ config(materialized='table') }}

SELECT
    CAST(kundennummer AS TEXT) AS "Id",
    CASE
        WHEN TRIM(unternehmensname) IS NOT NULL AND TRIM(unternehmensname) != '' THEN INITCAP(TRIM(unternehmensname))
        ELSE INITCAP('Customer ' || kundennummer)
    END AS "Name",
    CAST(erp_nr AS TEXT) AS "ERP_Number__c",
    CASE LOWER(TRIM(kundenklasse))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'silber' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    CAST(vertriebsgebiet AS TEXT) AS "Region__c",
    CAST(industrie AS TEXT) AS "Industry",
    CAST(homepage AS TEXT) AS "Website",
    CAST(stadt AS TEXT) AS "BillingCity",
    CAST(land_region AS TEXT) AS "BillingCountry",
    CAST(kundennummer AS TEXT) AS "Legacy_Customer_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}