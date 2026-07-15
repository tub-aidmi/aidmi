{{ config(materialized='table') }}

SELECT
  id AS "Id",
  COALESCE(INITCAP(NULLIF(TRIM(name), '')), 'Unknown') AS "Name",
  UPPER(TRIM(serial_number__c)) AS "Serial_Number__c",
  CASE 
    WHEN TRIM(warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY')::TEXT
    WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(warranty_end_date__c)
    WHEN TRIM(warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY')::TEXT
    ELSE NULL 
  END AS "Warranty_End_Date__c",
  TRIM(account__c) AS "Account__c",
  TRIM(project__c) AS "Project__c",
  id AS "Legacy_Asset_ID__c",
  '2024-01-01' AS "CreatedDate",
  '2024-01-01' AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}