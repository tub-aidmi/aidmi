{{ config(materialized='table') }}
WITH
default_account AS (
  SELECT MD5('CUST-M1001') AS account_id
),
default_project AS (
  SELECT MD5('PROJ-00001') AS project_id
),
asset_data AS (
  SELECT
    ma.asset_kennung,
    ma.asset_name,
    ma.serien_nummer,
    ma.garantieende,
    ma.kunden_kennung,
    ma.projekt_kennung
  FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
),
resolved_accounts AS (
  SELECT
    ad.asset_kennung,
    COALESCE(
      (SELECT MD5(mk.kundennummer) FROM {{ source('fixture_master_v2_src', 'master_kunden') }} mk WHERE mk.kundennummer = ad.kunden_kennung),
      (SELECT account_id FROM default_account)
    ) AS account_id
  FROM asset_data ad
),
resolved_projects AS (
  SELECT
    ad.asset_kennung,
    COALESCE(
      (SELECT MD5(mp.projekt_kennung) FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp WHERE mp.projekt_kennung = ad.projekt_kennung),
      (SELECT project_id FROM default_project)
    ) AS project_id
  FROM asset_data ad
)
SELECT
  MD5(ad.asset_kennung) AS "Id",
  INITCAP(TRIM(ad.asset_name)) AS "Name",
  TRIM(ad.serien_nummer) AS "Serial_Number__c",
  CASE
    WHEN ad.garantieende IS NULL OR ad.garantieende = 'N/A' OR ad.garantieende = '0000-00-00' THEN NULL
    WHEN ad.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
    WHEN ad.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN ad.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Warranty_End_Date__c",
  ra.account_id AS "Account__c",
  rp.project_id AS "Project__c",
  ad.asset_kennung AS "Legacy_Asset_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM asset_data ad
LEFT JOIN resolved_accounts ra ON ad.asset_kennung = ra.asset_kennung
LEFT JOIN resolved_projects rp ON ad.asset_kennung = rp.asset_kennung