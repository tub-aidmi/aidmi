{{ config(materialized='table') }}

SELECT 
  'ASSET_' || ma.asset_kennung AS "Id",
  COALESCE(TRIM(ma.asset_name), 'Unnamed Asset') AS "Name",
  TRIM(ma.serien_nummer) AS "Serial_Number__c",
  CASE 
    WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
      TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ma.garantieende
    ELSE NULL 
  END AS "Warranty_End_Date__c",
  'ACCT_' || mk.kundennummer AS "Account__c",
  'PROJ_' || mp.projekt_kennung AS "Project__c",
  ma.asset_kennung AS "Legacy_Asset_ID__c",
  CURRENT_TIMESTAMP::text AS "CreatedDate",
  CURRENT_TIMESTAMP::text AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} mp 
  ON ma.projekt_kennung = mp.projekt_kennung
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
  ON ma.kunden_kennung = mk.kundennummer