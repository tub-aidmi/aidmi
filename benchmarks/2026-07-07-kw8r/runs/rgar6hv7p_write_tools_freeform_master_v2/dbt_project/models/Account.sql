{{ config(materialized='table') }}

WITH kunden AS (
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
),

account_mapping AS (
    SELECT
        '001' || ENCODE(DIGEST(kundennummer, 'md5'), 'hex') AS Id,
        INITCAP(TRIM(unternehmensname)) AS Name,
        NULLIF(TRIM(erp_nr), '') AS ERP_Number__c,
        CASE 
            WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
            WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
            WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER', 'SILBER') THEN 'Silver'
            WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'BRONZE') THEN 'Bronze'
            ELSE NULL
        END AS Customer_Tier__c,
        NULLIF(TRIM(vertriebsgebiet), '') AS Region__c,
        NULLIF(TRIM(industrie), '') AS Industry,
        NULLIF(TRIM(homepage), '') AS Website,
        NULLIF(TRIM(stadt), '') AS BillingCity,
        NULLIF(TRIM(land_region), '') AS BillingCountry,
        kundennummer AS Legacy_Customer_ID__c,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS CreatedDate,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS LastModifiedDate,
        0 AS IsDeleted
    FROM kunden
)

SELECT
    Id,
    Name,
    ERP_Number__c,
    Customer_Tier__c,
    Region__c,
    Industry,
    Website,
    BillingCity,
    BillingCountry,
    Legacy_Customer_ID__c,
    CreatedDate,
    LastModifiedDate,
    IsDeleted
FROM account_mapping
