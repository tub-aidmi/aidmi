{{ config(materialized='table') }}

SELECT
  ia.id AS "Id",
  
   -- Name: trim and use default if null (NOT NULL target column)
  COALESCE(TRIM(ia.name), 'Unknown Asset') AS "Name",
  
   -- Serial number - direct mapping
  ia.serial_number__c AS "Serial_Number__c",
  
   -- Warranty end date: parse multiple formats to ISO YYYY-MM-DD consistently
  CASE 
    WHEN ia.warranty_end_date__c IS NULL THEN NULL
    WHEN ia.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(ia.warranty_end_date__c AS TEXT)
    WHEN ia.warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
      THEN TO_DATE(ia.warranty_end_date__c, 'MM/DD/YYYY')::TEXT
    WHEN ia.warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
      THEN TO_DATE(ia.warranty_end_date__c, 'DD.MM.YYYY')::TEXT
    WHEN ia.warranty_end_date__c ~ '^\d{8}$' 
      THEN TO_DATE(ia.warranty_end_date__c, 'YYYYMMDD')::TEXT
    ELSE NULL
  END AS "Warranty_End_Date__c",
  
   -- Account reference: standardized fallback join (erp_number first, then legacy_customer_id) — consistent with Project__c key resolution
  acct.id AS "Account__c",
  
   -- Project reference: resolve to Salesforce-style Project Id via direct id match
  proj.id AS "Project__c",
  
   -- Legacy_Asset_ID__: preserve source natural key for verification
  ia.id AS "Legacy_Asset_ID__c",
  
   -- Dates not available in source - explicit type cast per reviewer feedback
  CAST(NULL AS TEXT) AS "CreatedDate",
  CAST(NULL AS TEXT) AS "LastModifiedDate",
  
   -- IsDeleted: default to 0 (not deleted) since source doesn't track this
   0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} ia
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acct 
  ON ia.account__c = acct.erp_number__c OR ia.account__c = acct.legacy_customer_id__c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'project__c') }} proj 
  ON ia.project__c = proj.id