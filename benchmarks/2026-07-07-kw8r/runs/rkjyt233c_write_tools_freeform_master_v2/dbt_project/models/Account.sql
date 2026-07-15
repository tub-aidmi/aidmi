{{ config(materialized='table') }}

WITH customer_data AS (
  SELECT
    kundennummer AS customer_id,
    unternehmensname AS company_name,
    erp_nr AS erp_number,
    kundenklasse AS customer_tier,
    vertriebsgebiet AS region,
    industrie AS industry,
    homepage AS website,
    stadt AS city,
    land_region AS country
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
normalized_customers AS (
  SELECT
    customer_id,
    COALESCE(NULLIF(TRIM(company_name), ''), 'Unknown') AS company_name,
    TRIM(erp_number) AS erp_number,
    -- Normalize customer tier to enum: Gold, Silver, Bronze, Platinum
    CASE 
      WHEN UPPER(TRIM(customer_tier)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
      WHEN UPPER(TRIM(customer_tier)) IN ('SILVER', 'SILBER') THEN 'Silver'
      WHEN UPPER(TRIM(customer_tier)) IN ('BRONZE', 'BRONZE') THEN 'Bronze'
      WHEN UPPER(TRIM(customer_tier)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
      ELSE NULL
    END AS customer_tier,
    TRIM(region) AS region,
    INITCAP(TRIM(industry)) AS industry,
    TRIM(website) AS website,
    TRIM(city) AS city,
    TRIM(country) AS country
  FROM customer_data
)
SELECT
  customer_id AS "Id",
  company_name AS "Name",
  erp_number AS "ERP_Number__c",
  customer_tier AS "Customer_Tier__c",
  region AS "Region__c",
  industry AS "Industry",
  website AS "Website",
  city AS "BillingCity",
  country AS "BillingCountry",
  customer_id AS "Legacy_Customer_ID__c",
  CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
  CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM normalized_customers
