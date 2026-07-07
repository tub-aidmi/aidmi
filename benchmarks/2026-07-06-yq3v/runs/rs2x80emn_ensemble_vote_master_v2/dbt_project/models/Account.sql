{{ config(materialized='table') }}

SELECT
    mk.kundennummer AS "Id",
    COALESCE(TRIM(mk.unternehmensname), 'Unknown') AS "Name",
    TRIM(mk.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(mk.kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(mk.kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(mk.kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(mk.kundenklasse) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(mk.vertriebsgebiet) AS "Region__c",
    TRIM(mk.industrie) AS "Industry",
    TRIM(mk.homepage) AS "Website",
    TRIM(mk.stadt) AS "BillingCity",
    TRIM(mk.land_region) AS "BillingCountry",
    mk.kundennummer AS "Legacy_Customer_ID__c",
    '2000-01-01' AS "CreatedDate",
    '2000-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk