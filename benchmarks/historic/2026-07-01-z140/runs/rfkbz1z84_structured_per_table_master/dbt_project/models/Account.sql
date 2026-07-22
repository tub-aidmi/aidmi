{{ config(materialized='table') }}

SELECT 
    CAST(kundennummer AS text) AS "Id",
    INITCAP(TRIM(COALESCE(unternehmensname, ''))) AS "Name",
    CAST(erp_nr AS text) AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) = 'PLATINUM' THEN 'Platinum'
        WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) = 'SILBER' THEN 'Silver'
        WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) = 'BRONZE' THEN 'Bronze'
        ELSE INITCAP(TRIM(kundenklasse))
    END AS "Customer_Tier__c",
    CASE 
        WHEN TRIM(COALESCE(vertriebsgebiet, '')) = '' THEN NULL
        ELSE INITCAP(TRIM(vertriebsgebiet))
    END AS "Region__c",
    INITCAP(TRIM(COALESCE(industrie, ''))) AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    CAST(kundennummer AS text) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_kunden') }}