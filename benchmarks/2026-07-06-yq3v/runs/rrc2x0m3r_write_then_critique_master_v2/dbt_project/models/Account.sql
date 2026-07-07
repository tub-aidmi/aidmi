{{ config(materialized='table') }}

SELECT
    MD5(s.kundennummer) AS "Id",
    COALESCE(TRIM(s.unternehmensname), 'Unknown Account') AS "Name",
    TRIM(s.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(s.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(s.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(s.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(s.kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(s.vertriebsgebiet) AS "Region__c",
    TRIM(s.industrie) AS "Industry",
    TRIM(s.homepage) AS "Website",
    TRIM(s.stadt) AS "BillingCity",
    TRIM(s.land_region) AS "BillingCountry",
    TRIM(s.kundennummer) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS s
