{{ config(materialized='table') }}
WITH source_data AS (
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
  md5('ns:' || TRIM(kundennummer)) AS "Id",
  INITCAP(TRIM(unternehmensname)) AS "Name",
  TRIM(erp_nr) AS "ERP_Number__c",
  CASE
    WHEN UPPER(TRIM(kundenklasse)) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
    WHEN UPPER(TRIM(kundenklasse)) IN ('GOLD', 'GOLDEN') THEN 'Gold'
    WHEN UPPER(TRIM(kundenklasse)) IN ('SILBER', 'SILVER') THEN 'Silver'
    WHEN UPPER(TRIM(kundenklasse)) IN ('BRONZE', 'BRONZ') THEN 'Bronze'
    ELSE NULL
  END AS "Customer_Tier__c",
  INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
  INITCAP(TRIM(industrie)) AS "Industry",
  TRIM(homepage) AS "Website",
  INITCAP(TRIM(stadt)) AS "BillingCity",
  INITCAP(TRIM(land_region)) AS "BillingCountry",
  TRIM(kundennummer) AS "Legacy_Customer_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM source_data