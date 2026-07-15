{{ config(materialized='table') }}

SELECT
    'ACCT-' || LPAD(ROW_NUMBER() OVER (ORDER BY k.kundennummer)::TEXT, 6, '0') AS "Id",
    COALESCE(k.unternehmensname, 'Unknown') AS "Name",
    k.erp_nr AS "ERP_Number__c",
    CASE 
        WHEN UPPER(TRIM(k.kundenklasse)) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') 
            THEN INITCAP(LOWER(TRIM(k.kundenklasse)))
        WHEN UPPER(TRIM(k.kundenklasse)) = 'PLATIN' 
            THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    k.vertriebsgebiet AS "Region__c",
    k.industrie AS "Industry",
    k.homepage AS "Website",
    k.stadt AS "BillingCity",
    k.land_region AS "BillingCountry",
    k.kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k
