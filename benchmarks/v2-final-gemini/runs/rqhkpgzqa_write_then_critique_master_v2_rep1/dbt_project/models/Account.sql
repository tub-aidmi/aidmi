{{ config(materialized='table') }}

SELECT
    MD5(k.kundennummer) AS "Id",
    COALESCE(k.unternehmensname, 'Unnamed Account') AS "Name",
    k.erp_nr AS "ERP_Number__c",
    CASE LOWER(k.kundenklasse)
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    k.vertriebsgebiet AS "Region__c",
    k.industrie AS "Industry",
    k.homepage AS "Website",
    k.stadt AS "BillingCity",
    k.land_region AS "BillingCountry",
    k.kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
