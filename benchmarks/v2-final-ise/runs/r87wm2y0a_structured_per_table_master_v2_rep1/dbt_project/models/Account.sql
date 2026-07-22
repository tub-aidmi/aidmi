{{ config(materialized='table') }}

SELECT
    'ACCT-' || LPAD(REGEXP_REPLACE(kundennummer, '[^0-9]', '', 'g'), 5, '0') AS Id,
    COALESCE(INITCAP(TRIM(unternehmensname)), 'Customer ' || kundennummer) AS Name,
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kundenklasse)) = 'GOLD' OR TRIM(kundenklasse) = 'Gold' THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILBER', 'SILVER') OR TRIM(LOWER(kundenklasse)) = 'silver' THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'BRONZ') OR TRIM(LOWER(kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN', 'PLATINIUM') OR TRIM(LOWER(kundenklasse)) = 'platinum' THEN 'Platinum'
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    INITCAP(industrie) AS Industry,
    homepage AS Website,
    INITCAP(stadt) AS BillingCity,
    INITCAP(land_region) AS BillingCountry,
    kundennummer AS "Legacy_Customer_ID__c",
    CAST('2024-01-01' AS TEXT) AS CreatedDate,
    CAST('2024-01-01' AS TEXT) AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}