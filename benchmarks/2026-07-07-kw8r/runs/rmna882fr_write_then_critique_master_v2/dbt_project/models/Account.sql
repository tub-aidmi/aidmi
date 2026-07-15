{{ config(materialized='table') }}

SELECT
  CONCAT('001', SUBSTRING(MD5(TRIM(kundennummer)), 1, 15)) AS "Id",
  COALESCE(TRIM(unternehmensname), 'Unknown') AS "Name",
  TRIM(erp_nr) AS "ERP_Number__c",
  CASE 
    WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) = 'GOLD' THEN 'Gold'
    WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) = 'SILBER' THEN 'Silver'
    WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) IN ('BRONZE', 'BRONZ') THEN 'Bronze'
    WHEN UPPER(TRIM(COALESCE(kundenklasse, ''))) IN ('PLATIN', 'PLATINUM') THEN 'Platinum'
    ELSE NULL
  END AS "Customer_Tier__c",
  TRIM(vertriebsgebiet) AS "Region__c",
  TRIM(industrie) AS "Industry",
  TRIM(homepage) AS "Website",
  TRIM(stadt) AS "BillingCity",
  TRIM(land_region) AS "BillingCountry",
  TRIM(kundennummer) AS "Legacy_Customer_ID__c",
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}