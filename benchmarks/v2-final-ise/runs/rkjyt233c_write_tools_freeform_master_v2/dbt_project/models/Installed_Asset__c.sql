{{ config(materialized='table') }}
WITH asset_data AS (
  SELECT
    a.asset_kennung,
    TRIM(a.asset_name) AS name,
    a.serien_nummer AS serial_number,
    a.garantieende AS warranty_end_date,
    a.kunden_kennung AS customer_id,
    a.projekt_kennung AS project_id
  FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
),
normalized_assets AS (
  SELECT
    asset_kennung,
    name,
    TRIM(serial_number) AS serial_number,
    -- Parse warranty_end_date: handle YYYY-MM-DD, DD.MM.YYYY, MM/DD/YYYY
    CASE
      WHEN warranty_end_date IS NULL OR TRIM(warranty_end_date) = '' THEN NULL
      WHEN TRIM(warranty_end_date) = '0000-00-00' THEN NULL
      WHEN TRIM(warranty_end_date) ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CASE WHEN TO_DATE(TRIM(warranty_end_date), 'YYYY-MM-DD') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date), 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END
      WHEN TRIM(warranty_end_date) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
        CASE WHEN TO_DATE(TRIM(warranty_end_date), 'DD.MM.YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date), 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END
      WHEN TRIM(warranty_end_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
        CASE WHEN TO_DATE(TRIM(warranty_end_date), 'MM/DD/YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date), 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END
      ELSE NULL
    END AS warranty_end_date,
    customer_id,
    project_id
  FROM asset_data
)
SELECT
  asset_kennung AS "Id",
  COALESCE(NULLIF(name, ''), 'Untitled Asset') AS "Name",
  serial_number AS "Serial_Number__c",
  warranty_end_date AS "Warranty_End_Date__c",
  customer_id AS "Account__c",
  -- Map project_id to Project__c Id
  CASE
    WHEN project_id IS NULL THEN NULL
    WHEN project_id LIKE 'PROJ-M-%' THEN REPLACE(project_id, 'PROJ-M-', 'PROJ-')
    ELSE project_id
  END AS "Project__c",
  asset_kennung AS "Legacy_Asset_ID__c",
  CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
  CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM normalized_assets