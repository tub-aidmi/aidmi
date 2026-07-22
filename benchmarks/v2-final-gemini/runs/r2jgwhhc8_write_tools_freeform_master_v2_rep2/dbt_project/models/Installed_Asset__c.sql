{{
  config(
    materialized='table'
  )
}}

SELECT
  MD5(master_assets.asset_kennung) AS "Id",
  COALESCE(master_assets.asset_name, 'Unknown Asset') AS "Name",
  master_assets.serien_nummer AS "Serial_Number__c",
  COALESCE(
    TO_CHAR(TO_DATE(master_assets.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
    TO_CHAR(TO_DATE(master_assets.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
    TO_CHAR(TO_DATE(master_assets.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
  ) AS "Warranty_End_Date__c",
  MD5(master_assets.kunden_kennung) AS "Account__c", -- Assuming kunden_kennung links to master_kunden.kundennummer
  MD5(master_assets.projekt_kennung) AS "Project__c", -- Assuming projekt_kennung links to master_projekte.projekt_kennung
  master_assets.asset_kennung AS "Legacy_Asset_ID__c",
  NOW()::TEXT AS "CreatedDate",
  NOW()::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM
  {{ source('fixture_master_v2_src', 'master_assets') }} AS master_assets
