{{ config(materialized='table') }}
WITH parsed_dates AS (
  SELECT
    id,
    name,
    serial_number__c,
    warranty_end_date__c,
    account__c,
    project__c,
    CASE
      WHEN warranty_end_date__c ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty_end_date__c
      WHEN warranty_end_date__c ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'DD-MM-YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS parsed_warranty_end_date
  FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)
SELECT
  id AS "Id",
  COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
  TRIM(serial_number__c) AS "Serial_Number__c",
  parsed_warranty_end_date AS "Warranty_End_Date__c",
  account__c AS "Account__c",
  project__c AS "Project__c",
  id AS "Legacy_Asset_ID__c",
  CURRENT_TIMESTAMP::text AS "CreatedDate",
  CURRENT_TIMESTAMP::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM parsed_dates