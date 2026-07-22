{{ config(materialized='table') }}

WITH date_parsed AS (
  SELECT
    asset_kennung,
    garantieende,
    CASE
      WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende
      WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN garantieende ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS parsed_garantieende
  FROM {{ source('fixture_master_v2_src', 'master_assets') }}
)

SELECT
  a.asset_kennung AS "Id",
  TRIM(a.asset_name) AS "Name",
  a.serien_nummer AS "Serial_Number__c",
  dp.parsed_garantieende AS "Warranty_End_Date__c",
  k.kundennummer AS "Account__c",
  p.projekt_kennung AS "Project__c",
  a.asset_kennung AS "Legacy_Asset_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
LEFT JOIN date_parsed dp ON a.asset_kennung = dp.asset_kennung
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k ON a.kunden_kennung = k.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p ON a.projekt_kennung = p.projekt_kennung