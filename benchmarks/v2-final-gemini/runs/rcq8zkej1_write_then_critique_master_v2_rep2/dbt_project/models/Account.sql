{{
  config(materialized='table')
}}

SELECT
    MD5(k.kundennummer) AS "Id",
    COALESCE(k.unternehmensname, k.kundennummer) AS "Name",
    k.erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(k.kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(k.kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(k.kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(k.kundenklasse) = 'platin' THEN 'Platinum'
        WHEN LOWER(k.kundenklasse) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    k.vertriebsgebiet AS "Region__c",
    k.industrie AS "Industry",
    k.homepage AS "Website",
    k.stadt AS "BillingCity",
    k.land_region AS "BillingCountry",
    k.kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k