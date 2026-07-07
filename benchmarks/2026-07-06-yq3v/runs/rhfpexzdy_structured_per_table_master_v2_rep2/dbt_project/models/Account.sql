{{ config(materialized='table') }}

SELECT
    k.kundennummer AS "Id",
    COALESCE(k.unternehmensname, 'Unknown Account') AS "Name",
    k.erp_nr AS "ERP_Number__c",
    CASE
        WHEN k.kundenklasse ILIKE 'Gold' THEN 'Gold'
        WHEN k.kundenklasse ILIKE 'Silver' THEN 'Silver'
        WHEN k.kundenklasse ILIKE 'Bronze' THEN 'Bronze'
        WHEN k.kundenklasse ILIKE 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    k.vertriebsgebiet AS "Region__c",
    k.industrie AS "Industry",
    k.homepage AS "Website",
    k.stadt AS "BillingCity",
    k.land_region AS "BillingCountry",
    k.kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
