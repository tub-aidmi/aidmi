{{ config(materialized='table') }}

SELECT
    MD5(m.kundennummer) AS "Id",
    COALESCE(TRIM(m.unternehmensname), 'Unknown Account') AS "Name",
    m.erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(m.kundenklasse)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(m.kundenklasse)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(m.kundenklasse)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(m.kundenklasse)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    m.vertriebsgebiet AS "Region__c",
    m.industrie AS "Industry",
    m.homepage AS "Website",
    m.stadt AS "BillingCity",
    m.land_region AS "BillingCountry",
    m.kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS m