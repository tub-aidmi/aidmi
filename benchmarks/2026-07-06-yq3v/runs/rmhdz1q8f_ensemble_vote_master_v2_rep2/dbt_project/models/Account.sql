{{ config(materialized='table') }}

SELECT
    TRIM(k.kundennummer) AS "Id",
    COALESCE(TRIM(k.unternehmensname), 'Unknown Account Name') AS "Name",
    TRIM(k.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(k.kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(k.kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(k.kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(k.kundenklasse) IN ('platinum', 'platin') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(k.vertriebsgebiet) AS "Region__c",
    TRIM(k.industrie) AS "Industry",
    TRIM(k.homepage) AS "Website",
    TRIM(k.stadt) AS "BillingCity",
    TRIM(k.land_region) AS "BillingCountry",
    TRIM(k.kundennummer) AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
