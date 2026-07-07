{{ config(materialized='table') }}

SELECT
    k.kundennummer AS "Id",
    COALESCE(TRIM(k.unternehmensname), k.kundennummer) AS "Name",
    k.erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(k.kundenklasse)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(k.kundenklasse)) = 'SILBER' THEN 'Silver'
        WHEN UPPER(TRIM(k.kundenklasse)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(k.kundenklasse)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    k.vertriebsgebiet AS "Region__c",
    k.industrie AS "Industry",
    k.homepage AS "Website",
    k.stadt AS "BillingCity",
    k.land_region AS "BillingCountry",
    k.kundennummer AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
