{{ config(materialized='table') }}

SELECT
  -- Id: Use asset_kennung as the unique identifier
  CAST(asset_kennung AS TEXT) AS "Id",
  
  -- Name: Asset name from source
  CAST(asset_name AS TEXT) AS "Name",
  
  -- Serial_Number__c: Direct mapping
  CAST(serien_nummer AS TEXT) AS "Serial_Number__c",
  
  -- Warranty_End_Date__c: Parse multiple date formats to ISO YYYY-MM-DD
  CASE 
    -- Handle ISO format YYYY-MM-DD (but filter out invalid sentinel like 0000-00-00)
    WHEN garantieende IS NOT NULL 
         AND garantieende ~ '^\d{4}-\d{2}-\d{2}$' 
         AND garantieende != '0000-00-00' THEN garantieende
    
    -- Handle YYYYMMDD format (e.g., "20250828")
    WHEN garantieende IS NOT NULL 
         AND garantieende ~ '^\d{8}$' THEN 
      SUBSTRING(garantieende, 1, 4) || '-' || 
      SUBSTRING(garantieende, 5, 2) || '-' || 
      SUBSTRING(garantieende, 7, 2)
    
    -- Handle DD.MM.YYYY European format (e.g., "09.09.2023")
    WHEN garantieende IS NOT NULL 
         AND garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
      TO_CHAR(
        TO_DATE(garantieende, 'DD.MM.YYYY'), 
        'YYYY-MM-DD'
      )
    
    -- Handle MM/DD/YYYY US format (e.g., "2/26/2027")
    WHEN garantieende IS NOT NULL 
         AND garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
      TO_CHAR(
        TO_DATE(garantieende, 'MM/DD/YYYY'), 
        'YYYY-MM-DD'
      )
    
    -- Default to NULL for N/A, 0000-00-00, or any other invalid/unrecognized values
    ELSE NULL
  END AS "Warranty_End_Date__c",
  
  -- Account__c: Foreign key to Account ( Kundenkennung)
  CAST(kunden_kennung AS TEXT) AS "Account__c",
  
  -- Project__c: Foreign key to Project (Projektkennung)
  CAST(projekt_kennung AS TEXT) AS "Project__c",
  
  -- Legacy_Asset_ID__c: Original identifier before transformation
  CAST(asset_kennung AS TEXT) AS "Legacy_Asset_ID__c",
  
  -- CreatedDate: Not available in source data
  NULL::TEXT AS "CreatedDate",
  
  -- LastModifiedDate: Not available in source data
  NULL::TEXT AS "LastModifiedDate",
  
  -- IsDeleted: Default to 0 (not deleted)
  0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_assets') }}