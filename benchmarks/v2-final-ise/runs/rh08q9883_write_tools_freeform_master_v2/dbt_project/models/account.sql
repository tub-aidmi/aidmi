{{ config(materialized='table') }}

SELECT
    'A' || LPAD(SUBSTRING(kundennummer FROM '\d+')::INT, 8, '0') AS "Id",
    INITCAP(COALESCE(unternehmensname, kundennummer)) AS "Name",
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
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
