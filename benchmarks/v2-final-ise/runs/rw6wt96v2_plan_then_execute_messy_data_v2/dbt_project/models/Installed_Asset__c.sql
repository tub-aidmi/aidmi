{{ config(materialized='table') }}

WITH asset_data AS (
  SELECT
    id,
    name,
    serial_number__c,
    warranty_end_date__c,
    account__c,
    project__c
  FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
),

account_mapping AS (
  SELECT
    id AS account_id
  FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),

project_mapping AS (
  SELECT
    id AS project_id
  FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
)

SELECT
  ad.id AS "Id",
  COALESCE(INITCAP(TRIM(ad.name)), 'Unknown') AS "Name",
  ad.serial_number__c AS "Serial_Number__c",
  CASE
    WHEN ad.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN ad.warranty_end_date__c
    WHEN ad.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ad.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN ad.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ad.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN ad.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ad.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Warranty_End_Date__c",
  ad.account__c AS "Account__c",
  ad.project__c AS "Project__c",
  ad.id AS "Legacy_Asset_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM asset_data ad