{{
  config(
    materialized='table'
  )
}}

WITH asset_data AS (
  SELECT
    asset_kennung AS asset_id,
    asset_name AS asset_name,
    serien_nummer AS serial_number,
    garantieende AS warranty_end_date,
    kunden_kennung AS customer_id,
    projekt_kennung AS project_id
  FROM {{ source('fixture_master_src', 'master_assets') }}
)

SELECT
  asset_id AS Id,
  asset_name AS Name,
  serial_number AS Serial_Number__c,
  CASE
    WHEN warranty_end_date ~ '^\d{8}$' THEN
      TO_CHAR(TO_DATE(warranty_end_date, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN warranty_end_date ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
      TO_CHAR(TO_DATE(warranty_end_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN warranty_end_date ~ '^\d{4}-\d{2}-\d{2}$' THEN
      warranty_end_date
    ELSE NULL
  END AS Warranty_End_Date__c,
  customer_id AS Account__c,
  project_id AS Project__c,
  asset_id AS Legacy_Asset_ID__c,
  CURRENT_TIMESTAMP::text AS CreatedDate,
  CURRENT_TIMESTAMP::text AS LastModifiedDate,
  0 AS IsDeleted
FROM asset_data