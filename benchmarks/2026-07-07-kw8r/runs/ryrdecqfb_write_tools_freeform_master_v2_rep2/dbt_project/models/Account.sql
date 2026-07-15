{{ config(materialized='table') }}

SELECT
    'ACC_' || kundennummer AS "Id",
    unternehmensname AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE 
        WHEN UPPER(kundenklasse) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(kundenklasse) IN ('SILVER', 'SILBER') THEN 'Silver'
        WHEN UPPER(kundenklasse) IN ('BRONZE') THEN 'Bronze'
        WHEN UPPER(kundenklasse) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
