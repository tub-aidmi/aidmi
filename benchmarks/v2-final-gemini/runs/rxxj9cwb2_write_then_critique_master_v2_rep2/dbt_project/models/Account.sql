{{ config(materialized='table') }}

SELECT
    MD5(kc.kundennummer) AS "Id",
    COALESCE(kc.unternehmensname, 'Unknown Account') AS "Name",
    kc.erp_nr AS "ERP_Number__c",
    CASE
        WHEN kc.kundenklasse ILIKE 'Gold' THEN 'Gold'
        WHEN kc.kundenklasse ILIKE 'Silver' THEN 'Silver'
        WHEN kc.kundenklasse ILIKE 'Bronze' THEN 'Bronze'
        WHEN kc.kundenklasse ILIKE 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    kc.vertriebsgebiet AS "Region__c",
    kc.industrie AS "Industry",
    kc.homepage AS "Website",
    kc.stadt AS "BillingCity",
    kc.land_region AS "BillingCountry",
    kc.kundennummer AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kc
