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
  -- Generate deterministic Salesforce-style Id
  SUBSTRING(MD5('Account_' || kundennummer) FROM 1 FOR 18) AS "Id",
  
  -- Name: use unternehmensname if available, otherwise kundennummer
  COALESCE(NULLIF(TRIM(unternehmensname), ''), kundennummer) AS "Name",
  
  -- ERP Number
  NULLIF(TRIM(erp_nr), '') AS "ERP_Number__c",
  
  -- Customer Tier: normalize to enum values
  CASE 
    WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
    WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
    WHEN UPPER(TRIM(kundenklasse)) IN ('SILVER', 'SILBER') THEN 'Silver'
    WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'BRONZE') THEN 'Bronze'
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
  
  -- Legacy Customer ID
  kundennummer AS "Legacy_Customer_ID__c",
  
  -- CreatedDate: use current timestamp for now
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  
  -- LastModifiedDate: use current timestamp for now
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- IsDeleted: default to 0
  0 AS "IsDeleted"

FROM customer_data
