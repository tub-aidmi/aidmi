{{ config(materialized='table') }}

SELECT
    MD5(mk.kundennummer) AS "Id",
    COALESCE(TRIM(mk.unternehmensname), TRIM(mk.kundennummer), 'Unknown Account') AS "Name",
    mk.erp_nr AS "ERP_Number__c",
    CASE UPPER(TRIM(mk.kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    mk.vertriebsgebiet AS "Region__c",
    mk.industrie AS "Industry",
    mk.homepage AS "Website",
    mk.stadt AS "BillingCity",
    mk.land_region AS "BillingCountry",
    mk.kundennummer AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
