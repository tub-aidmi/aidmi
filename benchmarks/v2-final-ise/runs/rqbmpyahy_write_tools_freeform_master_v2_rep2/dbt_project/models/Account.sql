{{ config(materialized='table') }}

WITH customer_data AS (
  SELECT
    kundennummer,
    unternehmensname,
    erp_nr,
    kundenklasse,
    vertriebsgebiet,
    industrie,
    homepage,
    stadt,
    land_region
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
  -- Generate deterministic Salesforce-style ID from natural key
  '001' || SUBSTRING(MD5(kundennummer) FROM 1 FOR 15) AS "Id",
  
  -- Name: use unternehmensname if not null, otherwise generate from kundennummer
  COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Account ' || kundennummer) AS "Name",
  
  -- ERP Number
  NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
  
  -- Customer Tier: normalize kundenklasse
  CASE 
    WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
    WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER', 'SILBER') THEN 'Silver'
    WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE') THEN 'Bronze'
    WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
    ELSE NULL
  END AS "Customer_Tier__c",
  
  -- Region
  NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
  
  -- Industry
  NULLIF(TRIM(industrie), '') AS "Industry",
  
  -- Website
  NULLIF(TRIM(homepage), '') AS "Website",
  
  -- Billing City
  NULLIF(TRIM(stadt), '') AS "BillingCity",
  
  -- Billing Country
  NULLIF(TRIM(land_region), '') AS "BillingCountry",
  
  -- Legacy Customer ID from source natural key
  kundennummer AS "Legacy_Customer_ID__c",
  
  -- Timestamps (using current date as placeholder)
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- Not deleted
  0 AS "IsDeleted"

FROM customer_data
