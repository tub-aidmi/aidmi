{{ config(materialized='table') }}

SELECT
    'A-' || RIGHT('0000' || REGEXP_REPLACE(kundennummer, '[^0-9]', '', 'g'), 4) AS "Id",
    COALESCE(TRIM(unternehmensname), 'Unknown Account') AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE LOWER(TRIM(COALESCE(kundenklasse, '')))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'silber' THEN 'Silver'
        WHEN 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    CASE WHEN TRIM(COALESCE(vertriebsgebiet, '')) = '' THEN NULL ELSE TRIM(vertriebsgebiet) END AS "Region__c",
    CASE LOWER(TRIM(COALESCE(industrie, '')))
        WHEN 'finanzen' THEN 'Finance'
        WHEN 'technologie' THEN 'Technology'
        WHEN 'industrie' THEN 'Industrial'
        ELSE INITCAP(TRIM(COALESCE(industrie, '')))
    END AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_kunden') }}