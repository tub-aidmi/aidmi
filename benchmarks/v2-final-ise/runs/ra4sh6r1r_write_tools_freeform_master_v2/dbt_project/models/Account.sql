{{ config(materialized='table') }}

WITH customer_data AS (
  SELECT
    '' || MD5(COALESCE(kundennummer, '') || COALESCE(unternehmensname, '')) AS account_id,
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown') AS account_name,
    TRIM(erp_nr) AS erp_number,
    CASE
      WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
      WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
      WHEN UPPER(TRIM(kundenklasse)) IN ('SILBER', 'SILVER') THEN 'Silver'
      WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'BRONZE') THEN 'Bronze'
      ELSE NULL
    END AS customer_tier,
    TRIM(vertriebsgebiet) AS region,
    TRIM(industrie) AS industry,
    TRIM(homepage) AS website,
    TRIM(stadt) AS billing_city,
    TRIM(land_region) AS billing_country,
    TRIM(kundennummer) AS legacy_customer_id,
    '2024-01-01' AS created_date,
    '2024-01-01' AS last_modified_date,
    0 AS is_deleted
  FROM {{ source(source_slug, 'master_kunden') }}
)

SELECT
  account_id AS "Id",
  account_name AS "Name",
  erp_number AS "ERP_Number__c",
  customer_tier AS "Customer_Tier__c",
  region AS "Region__c",
  industry AS "Industry",
  website AS "Website",
  billing_city AS "BillingCity",
  billing_country AS "BillingCountry",
  legacy_customer_id AS "Legacy_Customer_ID__c",
  created_date AS "CreatedDate",
  last_modified_date AS "LastModifiedDate",
  is_deleted AS "IsDeleted"
FROM customer_data