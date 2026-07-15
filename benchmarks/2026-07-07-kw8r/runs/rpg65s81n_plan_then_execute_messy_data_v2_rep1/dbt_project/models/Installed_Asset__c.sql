{{ config(materialized='table') }}

WITH cleaned_assets AS (
  SELECT
    id,
    name,
    serial_number__c,
    warranty_end_date__c,
    account__c,
    project__c
  FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)

SELECT
  UPPER(REGEXP_REPLACE(TRIM(COALESCE(c.id, '')), '[^A-Za-z0-9]', '')) AS "Id",
  INITCAP(TRIM(COALESCE(c.name, 'Unnamed Asset'))) AS "Name",
  TRIM(c.serial_number__c) AS "Serial_Number__c",
  CASE 
    WHEN c.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
      TO_DATE(c.warranty_end_date__c, 'DD.MM.YYYY')::TEXT
    WHEN c.warranty_end_date__c ~ '^\d{8}$' THEN 
      TO_DATE(c.warranty_end_date__c, 'YYYYMMDD')::TEXT
    WHEN c.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN 
      TO_DATE(c.warranty_end_date__c, 'YYYY-MM-DD')::TEXT
    ELSE NULL
  END AS "Warranty_End_Date__c",
  a.id AS "Account__c",
  p.id AS "Project__c",
  c.id AS "Legacy_Asset_ID__c",
  CURRENT_DATE::TEXT AS "CreatedDate",
  CURRENT_DATE::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM cleaned_assets c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a 
  ON UPPER(REGEXP_REPLACE(TRIM(COALESCE(c.account__c, '')), '[^A-Za-z0-9]', '')) = UPPER(REGEXP_REPLACE(TRIM(COALESCE(a.id, '')), '[^A-Za-z0-9]', ''))
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'project__c') }} p 
  ON UPPER(REGEXP_REPLACE(TRIM(COALESCE(c.project__c, '')), '[^A-Za-z0-9]', '')) = UPPER(REGEXP_REPLACE(TRIM(COALESCE(p.id, '')), '[^A-Za-z0-9]', ''))