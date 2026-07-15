{{ config(materialized='table') }}
WITH source_data AS (
  SELECT
    asset_kennung,
    asset_name,
    serien_nummer,
    garantieende,
    kunden_kennung,
    projekt_kennung
  FROM {{ source('fixture_master_v2_src', 'master_assets') }}
)
SELECT
  gen_random_uuid()::text AS "Id",
  INITCAP(TRIM(asset_name)) AS "Name",
  TRIM(serien_nummer) AS "Serial_Number__c",
  CASE
    WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende
    WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Warranty_End_Date__c",
  CASE WHEN TRIM(kunden_kennung) IS NOT NULL THEN md5('ns:' || TRIM(kunden_kennung)) ELSE NULL END AS "Account__c",
  CASE WHEN TRIM(projekt_kennung) IS NOT NULL THEN md5('ns:' || TRIM(projekt_kennung)) ELSE NULL END AS "Project__c",
  TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM source_data