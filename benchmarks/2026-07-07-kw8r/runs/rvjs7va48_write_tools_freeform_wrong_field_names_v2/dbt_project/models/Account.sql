{{ config(materialized='table') }}

WITH customer_data AS (
  SELECT
    kunden_nr AS legacy_customer_id,
    firmenname,
    erp_nummer,
    kategorie,
    gebiet,
    branche,
    webseite,
    ort,
    land
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT
  legacy_customer_id AS "Id",
  INITCAP(TRIM(firmenname)) AS "Name",
  erp_nummer AS "ERP_Number__c",
  CASE 
    WHEN UPPER(TRIM(kategorie)) IN ('GOLD', 'SILBER') THEN 'Gold'
    WHEN UPPER(TRIM(kategorie)) IN ('SILVER') THEN 'Silver'
    WHEN UPPER(TRIM(kategorie)) IN ('BRONZE') THEN 'Bronze'
    WHEN UPPER(TRIM(kategorie)) IN ('PLATIN', 'PLATINUM') THEN 'Platinum'
    ELSE NULL
  END AS "Customer_Tier__c",
  INITCAP(TRIM(gebiet)) AS "Region__c",
  INITCAP(TRIM(branche)) AS "Industry",
  webseite AS "Website",
  ort AS "BillingCity",
  land AS "BillingCountry",
  legacy_customer_id AS "Legacy_Customer_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM customer_data