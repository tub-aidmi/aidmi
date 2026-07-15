{{ config(materialized='table') }}

SELECT 
    '001' || UPPER(SUBSTR(MD5(kundennummer), 1, 12)) AS "Id",
    INITCAP(TRIM(unternehmensname)) AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    INITCAP(TRIM(kundenklasse)) AS "Customer_Tier__c",
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    INITCAP(TRIM(industrie)) AS "Industry",
    homepage AS "Website",
    INITCAP(TRIM(stadt)) AS "BillingCity",
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
WHERE kundennummer IS NOT NULL 
  AND TRIM(kundennummer) != ''