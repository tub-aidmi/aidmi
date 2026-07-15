{{ config(materialized='table') }}

SELECT 
    'CUST-' || kundennummer AS "Id",
    COALESCE(INITCAP(TRIM(unternehmensname)), 'Unknown') AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(kundenklasse)) IN ('gold', '1') THEN 'Gold'
        WHEN LOWER(TRIM(kundenklasse)) IN ('silver', '2') THEN 'Silver'
        WHEN LOWER(TRIM(kundenklasse)) IN ('bronze', '3') THEN 'Bronze'
        ELSE 'Platinum' 
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    CASE 
        WHEN homepage LIKE 'http%' THEN homepage 
        ELSE 'https://' || COALESCE(homepage, '') 
    END AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}