{{ config(materialized='table') }}

SELECT
    MD5(mk.kundennummer) AS "Id",
    COALESCE(mk.unternehmensname, 'Unknown Account') AS "Name",
    mk.erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(mk.kundenklasse) LIKE '%gold%' THEN 'Gold'
        WHEN LOWER(mk.kundenklasse) LIKE '%silver%' THEN 'Silver'
        WHEN LOWER(mk.kundenklasse) LIKE '%bronze%' THEN 'Bronze'
        WHEN LOWER(mk.kundenklasse) LIKE '%platinum%' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    mk.vertriebsgebiet AS "Region__c",
    mk.industrie AS "Industry",
    mk.homepage AS "Website",
    mk.stadt AS "BillingCity",
    mk.land_region AS "BillingCountry",
    mk.kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} mk
