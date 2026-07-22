{{ config(materialized='table') }}
WITH asset_data AS (
  SELECT
    ma.asset_kennung,
    ma.asset_name,
    ma.serien_nummer,
    ma.garantieende,
    ma.kunden_kennung,
    ma.projekt_kennung,
    mk.kundennummer,
    mp.projekt_kennung AS projekt_kennung_join
  FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
  LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
    ON ma.kunden_kennung = mk.kundennummer AND ma.kunden_kennung LIKE 'CUST-%'
  LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} mp
    ON ma.projekt_kennung = mp.projekt_kennung AND ma.projekt_kennung LIKE 'PROJ-%' AND ma.projekt_kennung NOT LIKE 'PROJ-M-%'
)
SELECT
  MD5(ad.asset_kennung) AS "Id",
  INITCAP(TRIM(ad.asset_name)) AS "Name",
  TRIM(ad.serien_nummer) AS "Serial_Number__c",
  CASE
    WHEN ad.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ad.garantieende
    WHEN ad.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN ad.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN ad.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN ad.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Warranty_End_Date__c",
  CASE WHEN ad.kundennummer IS NOT NULL THEN MD5(ad.kundennummer) ELSE NULL END AS "Account__c",
  CASE WHEN ad.projekt_kennung_join IS NOT NULL THEN MD5(ad.projekt_kennung_join) ELSE NULL END AS "Project__c",
  ad.asset_kennung AS "Legacy_Asset_ID__c",
  '2023-01-01T00:00:00Z' AS "CreatedDate",
  '2023-01-01T00:00:00Z' AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM asset_data ad