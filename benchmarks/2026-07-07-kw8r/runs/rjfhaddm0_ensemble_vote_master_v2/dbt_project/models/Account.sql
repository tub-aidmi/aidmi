{{ config(materialized='table') }}

SELECT 
    '001' || LPAD(kundennummer, 15, '0') AS "Id",
    COALESCE(TRIM(unternehmensname), 'Unknown Company') AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE LOWER(TRIM(kundenklasse))
        WHEN 'gold'      THEN 'Gold'
        WHEN 'silber'    THEN 'Silver'
        WHEN 'bronze'    THEN 'Bronze'
        WHEN 'platin'    THEN 'Platinum'
        ELSE 'Bronze'
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}