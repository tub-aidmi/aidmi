{{ config(materialized='table') }}

SELECT
    kundennummer AS "Id",
    COALESCE(TRIM(unternehmensname), 'Unknown Account') AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kundenklasse)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) = 'SILBER' THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kunden') }}
