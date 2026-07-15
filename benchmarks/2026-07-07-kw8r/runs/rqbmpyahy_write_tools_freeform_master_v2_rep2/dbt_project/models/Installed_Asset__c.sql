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
    kundennummer,
    '001' || SUBSTRING(MD5(kundennummer) FROM 1 FOR 15) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

project_mapping AS (
  SELECT 
    projekt_kennung,
    '009' || SUBSTRING(MD5(projekt_kennung) FROM 1 FOR 15) AS project_id
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
  -- Generate deterministic Salesforce-style ID from natural key
  '019' || SUBSTRING(MD5(a.asset_kennung) FROM 1 FOR 15) AS "Id",
  
  -- Name (required)
  COALESCE(NULLIF(TRIM(a.asset_name), ''), 'Asset ' || a.asset_kennung) AS "Name",
  
  -- Serial Number
  NULLIF(TRIM(a.serien_nummer), '') AS "Serial_Number__c",
  
  -- Warranty End Date: parse various date formats
  CASE 
    WHEN a.garantieende IS NULL OR a.garantieende IN ('N/A', 'None', '') THEN NULL
    WHEN a.garantieende = '0000-00-00' THEN NULL
    WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN 
      CASE 
        WHEN CAST(SUBSTRING(a.garantieende FROM 6 FOR 2) AS INTEGER) BETWEEN 1 AND 12 AND
             CAST(SUBSTRING(a.garantieende FROM 9 FOR 2) AS INTEGER) BETWEEN 1 AND 31
        THEN a.garantieende
        ELSE NULL
      END
    WHEN a.garantieende ~ '^\d{8}$' THEN 
      TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
      TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN 
      TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YY'), 'YYYY-MM-DD')
    WHEN a.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
      TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN a.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{2}$' THEN 
      TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YY'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Warranty_End_Date__c",
  
  -- Account__c: join to customer
  CASE 
    WHEN a.kunden_kennung LIKE 'CUST-M%' THEN am.account_id
    ELSE NULL
  END AS "Account__c",
  
  -- Project__c: join to project
  CASE 
    WHEN a.projekt_kennung LIKE 'PROJ-%' THEN pm.project_id
    ELSE NULL
  END AS "Project__c",
  
  -- Legacy Asset ID from source natural key
  a.asset_kennung AS "Legacy_Asset_ID__c",
  
  -- Timestamps
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- Not deleted
  0 AS "IsDeleted"

FROM asset_data a
LEFT JOIN account_mapping am ON a.kunden_kennung = am.kundennummer
LEFT JOIN project_mapping pm ON a.projekt_kennung = pm.projekt_kennung
