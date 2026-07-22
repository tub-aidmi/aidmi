{{ config(materialized='table') }}

SELECT
    'ACCT-' || kundennummer AS "Id",
    INITCAP(TRIM(COALESCE(unternehmensname, 'Unknown'))) AS "Name",
    CAST(erp_nr AS TEXT) AS "ERP_Number__c",
    CASE UPPER(TRIM(kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATIN' THEN 'Platinum'
        ELSE kundenklasse  -- fallback to original
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    CASE UPPER(TRIM(industrie))
        WHEN 'IT' THEN 'Technology'
        WHEN 'TECHNOLOGIE' THEN 'Technology'
        WHEN 'INDUSTRIE' THEN 'Manufacturing'
        WHEN 'FINANZEN' THEN 'Finance'
        WHEN 'GESUNDHEITSWESEN' THEN 'Healthcare'
        WHEN 'GESUNDHEIT' THEN 'Healthcare'
        WHEN 'CONSTRUCTION' THEN 'Construction'
        WHEN 'BAU' THEN 'Construction'
        ELSE industrie  -- pass-through for English or already-correct values
    END AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CAST(COALESCE(NULLIF(TO_DATE(TRIM(garantieende), 'YYYY-MM-DD'), '1900-01-01')::TEXT, NULL) AS TEXT) AS "CreatedDate",
    NOW()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM "{{ source('fixture_master_v2_src', 'master_kunden') }}"