{{ config(materialized='table') }}

WITH kunden_data AS (
  SELECT
    kunden_nr,
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
  kunden_nr AS "Id",
  firmenname AS "Name",
  erp_nummer AS "ERP_Number__c",
  CASE
    WHEN UPPER(TRIM(kategorie)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
    WHEN UPPER(TRIM(kategorie)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
    WHEN UPPER(TRIM(kategorie)) IN ('SILBER', 'SILVER') THEN 'Silver'
    WHEN UPPER(TRIM(kategorie)) IN ('BRONZE') THEN 'Bronze'
    ELSE NULL
  END AS "Customer_Tier__c",
  INITCAP(TRIM(gebiet)) AS "Region__c",
  INITCAP(TRIM(branche)) AS "Industry",
  LOWER(TRIM(webseite)) AS "Website",
  TRIM(ort) AS "BillingCity",
  TRIM(land) AS "BillingCountry",
  kunden_nr AS "Legacy_Customer_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM kunden_data
