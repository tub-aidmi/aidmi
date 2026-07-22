{{ config(materialized='table') }}

WITH asset_data AS (
  SELECT
    asset_kennung,
    asset_name,
    serien_nummer,
    garantieende,
    kunden_kennung,
    projekt_kennung
  FROM {{ source('fixture_master_v2_src', 'master_assets') }}
),

account_mapping AS (
  SELECT
    kundennummer AS legacy_customer_id,
    SUBSTRING(MD5('Account_' || kundennummer) FROM 1 FOR 18) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

project_mapping AS (
  SELECT
    projekt_kennung AS legacy_project_id,
    SUBSTRING(MD5('Project_' || projekt_kennung) FROM 1 FOR 18) AS project_id
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
),

-- Parse dates from various formats
parsed_dates AS (
  SELECT
    asset_kennung,
    asset_name,
    serien_nummer,
    kunden_kennung,
    projekt_kennung,
    -- Parse garantieende from various formats
    CASE 
      WHEN garantieende IS NULL OR TRIM(garantieende) IN ('', 'N/A', '0000-00-00') THEN NULL
      WHEN garantieende ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 
        TO_CHAR(TO_DATE(garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
      WHEN garantieende ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN 
        TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN garantieende ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN 
        TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHEN garantieende ~ '^[0-9]{8}$' THEN 
        TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN garantieende ~ '^[0-9]{4}[0-9]{2}[0-9]{2}$' THEN 
        TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
      ELSE NULL
    END AS warranty_end_date
  FROM asset_data
)

SELECT
  -- Generate deterministic Salesforce-style Id
  SUBSTRING(MD5('Asset_' || a.asset_kennung) FROM 1 FOR 18) AS "Id",
  
  -- Name: use asset_name
  COALESCE(NULLIF(TRIM(a.asset_name), ''), a.asset_kennung) AS "Name",
  
  -- Serial Number
  NULLIF(TRIM(a.serien_nummer), '') AS "Serial_Number__c",
  
  -- Warranty End Date
  a.warranty_end_date AS "Warranty_End_Date__c",
  
  -- Account__c: lookup from master_kunden via kunden_kennung
  am.account_id AS "Account__c",
  
  -- Project__c: lookup from master_projekte via projekt_kennung
  pm.project_id AS "Project__c",
  
  -- Legacy Asset ID
  a.asset_kennung AS "Legacy_Asset_ID__c",
  
  -- CreatedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  
  -- LastModifiedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- IsDeleted: default to 0
  0 AS "IsDeleted"

FROM parsed_dates a
LEFT JOIN account_mapping am ON a.kunden_kennung = am.legacy_customer_id
LEFT JOIN project_mapping pm ON REPLACE(a.projekt_kennung, 'PROJ-M-', 'PROJ-') = pm.legacy_project_id
