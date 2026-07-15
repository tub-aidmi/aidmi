{{ config(materialized='table') }}

SELECT
    '001' || k."kundennummer" AS "Id",
    COALESCE(NULLIF(TRIM(k."unternehmensname"), ''), 'Unknown Account') AS "Name",
    k."erp_nr" AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('BRONZE') THEN 'Bronze'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('PLATIN', 'PLATINUM') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    k."vertriebsgebiet" AS "Region__c",
    CASE 
        WHEN UPPER(TRIM(k."industrie")) IN ('GESUNDHEITSWESEN', 'HEALTHCARE') THEN 'Healthcare'
        WHEN UPPER(TRIM(k."industrie")) IN ('FINANZEN', 'FINANCE') THEN 'Finance'
        WHEN UPPER(TRIM(k."industrie")) IN ('TECHNOLOGIE', 'TECHNOLOGY') THEN 'Technology'
        WHEN UPPER(TRIM(k."industrie")) IN ('IT') THEN 'IT'
        WHEN UPPER(TRIM(k."industrie")) IN ('INDUSTRIE', 'MANUFACTURING') THEN 'Manufacturing'
        ELSE INITCAP(TRIM(k."industrie"))
    END AS "Industry",
    k."homepage" AS "Website",
    k."stadt" AS "BillingCity",
    k."land_region" AS "BillingCountry",
    k."kundennummer" AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k