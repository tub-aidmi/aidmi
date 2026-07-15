{{ config(materialized='table') }}

WITH asset_data AS (
  SELECT
    asset.asset_id AS legacy_asset_id,
    asset.bezeichnung,
    asset.seriennr,
    asset.garantie_bis,
    asset.kd_ref AS customer_id,
    asset.projekt_ref AS project_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} asset
)

SELECT
  legacy_asset_id AS "Id",
  INITCAP(TRIM(bezeichnung)) AS "Name",
  TRIM(seriennr) AS "Serial_Number__c",
  CASE 
    WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
    WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Warranty_End_Date__c",
  customer_id AS "Account__c",
  project_id AS "Project__c",
  legacy_asset_id AS "Legacy_Asset_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM asset_data