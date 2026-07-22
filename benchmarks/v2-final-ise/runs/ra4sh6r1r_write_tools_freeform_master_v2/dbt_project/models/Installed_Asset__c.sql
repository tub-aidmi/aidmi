{{ config(materialized='table') }}

WITH asset_data AS (
  SELECT 
    '' || MD5(COALESCE(a.asset_kennung, '') || COALESCE(a.asset_name, '')) AS asset_id,
    
    -- Join to customer to get Account__c
    '' || MD5(COALESCE(kd.kundennummer, '') || COALESCE(kd.unternehmensname, '')) AS account_id,
    
    -- Join to project to get Project__c
    CASE 
      WHEN p.projekt_kennung IS NOT NULL THEN 
        '' || MD5(COALESCE(p.projekt_kennung, '') || COALESCE(p.projektname, ''))
      ELSE NULL
    END AS project_id,
    
    COALESCE(NULLIF(TRIM(a.asset_name), ''), 'Unknown Asset') AS asset_name,
    TRIM(a.serien_nummer) AS serial_number,
    
    -- Parse garantieende (warranty end date)
    CASE 
      WHEN TRIM(a.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(a.garantieende)
      WHEN TRIM(a.garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
        TO_CHAR(TO_DATE(TRIM(a.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN TRIM(a.garantieende) ~ '^\d{4}\d{2}\d{2}$' THEN 
        TO_CHAR(TO_DATE(TRIM(a.garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN TRIM(a.garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
        TO_CHAR(TO_DATE(TRIM(a.garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS warranty_end_date,
    
    TRIM(a.asset_kennung) AS legacy_asset_id,
    '2024-01-01' AS created_date,
    '2024-01-01' AS last_modified_date,
    0 AS is_deleted
    
  FROM {{ source(source_slug, 'master_assets') }} a
  LEFT JOIN {{ source(source_slug, 'master_kunden') }} kd 
    ON TRIM(a.kunden_kennung) = TRIM(kd.kundennummer)
  LEFT JOIN {{ source(source_slug, 'master_projekte') }} p 
    ON TRIM(a.projekt_kennung) = TRIM(p.projekt_kennung)
)

SELECT 
  asset_id AS "Id",
  asset_name AS "Name",
  serial_number AS "Serial_Number__c",
  warranty_end_date AS "Warranty_End_Date__c",
  account_id AS "Account__c",
  project_id AS "Project__c",
  legacy_asset_id AS "Legacy_Asset_ID__c",
  created_date AS "CreatedDate",
  last_modified_date AS "LastModifiedDate",
  is_deleted AS "IsDeleted"

FROM asset_data
