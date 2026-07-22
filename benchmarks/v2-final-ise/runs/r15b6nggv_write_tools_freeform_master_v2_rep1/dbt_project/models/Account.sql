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
    COALESCE(
        NULLIF(TRIM(unternehmensname), ''),
        'Unknown Account'
    ) AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD') THEN 'Gold'
        WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER', 'SILBER') THEN 'Silver'
        WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM customer_data
