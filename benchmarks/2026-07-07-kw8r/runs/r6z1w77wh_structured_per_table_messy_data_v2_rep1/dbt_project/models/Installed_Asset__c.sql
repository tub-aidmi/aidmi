{{ config(materialized='table') }}

SELECT
  id AS "Id",
  COALESCE(name, 'Asset_' || id) AS "Name",
  serial_number__c AS "Serial_Number__c",
  CASE
    WHEN warranty_end_date__c IS NOT NULL AND TRIM(warranty_end_date__c) != ''
    THEN CASE
      WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN TO_DATE(TRANSLATE(warranty_end_date__c, '.', '/'), 'DD/MM/YYYY')::TEXT
      WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT
      WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$'
        THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
      ELSE NULL
    END
    ELSE NULL
  END AS "Warranty_End_Date__c",
  account__c AS "Account__c",
  project__c AS "Project__c",
  id AS "Legacy_Asset_ID__c",
  CAST(NULL AS TEXT) AS "CreatedDate",
  CAST(NULL AS TEXT) AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}