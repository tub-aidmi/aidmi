{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kunden.kundennummer)) AS "Id",
    COALESCE(TRIM(kunden.unternehmensname), 'Unknown Account ' || TRIM(kunden.kundennummer)) AS "Name",
    TRIM(kunden.erp_nr) AS "ERP_Number__c",
    CASE TRIM(LOWER(kunden.kundenklasse))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunden.vertriebsgebiet) AS "Region__c",
    TRIM(kunden.industrie) AS "Industry",
    TRIM(kunden.homepage) AS "Website",
    TRIM(kunden.stadt) AS "BillingCity",
    TRIM(kunden.land_region) AS "BillingCountry",
    TRIM(kunden.kundennummer) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
