{{ config(materialized='table') }}

SELECT
    MD5(kundennummer) AS "Id",
    unternehmensname AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(kundenklasse) IN ('gold') THEN 'Gold'
        WHEN LOWER(kundenklasse) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(kundenklasse) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(kundenklasse) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }}
