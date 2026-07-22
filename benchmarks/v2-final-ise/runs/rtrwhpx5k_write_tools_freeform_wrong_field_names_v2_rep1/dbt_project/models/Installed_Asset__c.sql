{{ config(materialized='table') }}

WITH assets_data AS (
  SELECT
    asset_id,
    bezeichnung,
    seriennr,
    garantie_bis,
    kd_ref,
    projekt_ref
  FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
),

account_mapping AS (
  SELECT
    kunden_nr AS "AccountId",
    kunden_nr AS "Legacy_Customer_ID__c"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

project_mapping AS (
  SELECT
    proj_id AS "ProjectId",
    proj_id AS "Legacy_Project_ID__c"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
  asset_id AS "Id",
  bezeichnung AS "Name",
  seriennr AS "Serial_Number__c",
  CASE
    WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
    WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Warranty_End_Date__c",
  account_mapping."AccountId" AS "Account__c",
  project_mapping."ProjectId" AS "Project__c",
  asset_id AS "Legacy_Asset_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM assets_data
LEFT JOIN account_mapping ON assets_data.kd_ref = account_mapping."Legacy_Customer_ID__c"
LEFT JOIN project_mapping ON assets_data.projekt_ref = project_mapping."Legacy_Project_ID__c"
