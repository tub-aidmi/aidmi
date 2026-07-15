{{ config(materialized='table') }}

SELECT
    'ACCT-' || TRIM(kundennummer) AS "Id",
    INITCAP(TRIM(COALESCE(unternehmensname, 'Unknown'))) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE UPPER(TRIM(COALESCE(kundenklasse, '')))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATIN' THEN 'Platinum'
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'SILVER' THEN 'Silver'
        ELSE kundenklasse
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    CASE UPPER(TRIM(COALESCE(industrie, '')))
        WHEN 'IT' THEN 'Technology'
        WHEN 'TECHNOLOGIE' THEN 'Technology'
        WHEN 'INDUSTRIE' THEN 'Manufacturing'
        WHEN 'FINANZEN' THEN 'Finance'
        WHEN 'GESUNDHEITSWESEN' THEN 'Healthcare'
        WHEN 'GESUNDHEIT' THEN 'Healthcare'
        WHEN 'CONSTRUCTION' THEN 'Construction'
        WHEN 'BAU' THEN 'Construction'
        ELSE industrie
    END AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}