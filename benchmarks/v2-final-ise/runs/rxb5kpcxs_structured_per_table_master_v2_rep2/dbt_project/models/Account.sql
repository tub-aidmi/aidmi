{{ config(materialized='table') }}

SELECT 
    CONCAT('001', COALESCE(kundennummer, '')) AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), INITCAP(TRIM(kundennummer))) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) IN ('platinum', 'platin') THEN 'Platinum'
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) = 'bronze' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    homepage AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}