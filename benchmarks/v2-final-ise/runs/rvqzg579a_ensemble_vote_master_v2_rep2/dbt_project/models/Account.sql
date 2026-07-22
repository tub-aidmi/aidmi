{{ config(materialized='table') }}

SELECT
    MD5(kundennummer) AS "Id",
    unternehmensname AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(kundenklasse) IN ('PREMIUM', 'PLATINUM') THEN 'Platinum'
        WHEN UPPER(kundenklasse) IN ('GOLD', 'HIGH') THEN 'Gold'
        WHEN UPPER(kundenklasse) IN ('SILVER', 'MEDIUM') THEN 'Silver'
        WHEN UPPER(kundenklasse) IN ('BRONZE', 'LOW') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    INITCAP(land_region) AS "Region__c",
    INITCAP(industrie) AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    INITCAP(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }}