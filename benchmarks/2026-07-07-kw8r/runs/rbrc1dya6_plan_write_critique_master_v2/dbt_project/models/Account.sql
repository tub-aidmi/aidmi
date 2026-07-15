{{ config(materialized='table') }}

SELECT 
    'A0XX' || TRIM(kundennummer) AS "Id",
    TRIM(unternehmensname) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE LOWER(TRIM(kundenklasse))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}