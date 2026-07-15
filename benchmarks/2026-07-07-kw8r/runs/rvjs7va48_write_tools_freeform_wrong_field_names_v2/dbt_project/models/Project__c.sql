{{ config(materialized='table') }}

WITH project_data AS (
  SELECT
    proj.proj_id AS legacy_project_id,
    proj.name,
    proj.status,
    proj.go_live,
    proj.kd AS customer_id,
    proj.opp AS opportunity_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj
)

SELECT
  legacy_project_id AS "Id",
  INITCAP(TRIM(name)) AS "Name",
  CASE 
    WHEN UPPER(TRIM(status)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
    WHEN UPPER(TRIM(status)) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
    WHEN UPPER(TRIM(status)) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
    WHEN UPPER(TRIM(status)) IN ('PAUSIERT', 'ON HOLD') THEN 'On Hold'
    WHEN UPPER(TRIM(status)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE 
    WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
    WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Go_Live_Date__c",
  customer_id AS "Account__c",
  opportunity_id AS "Opportunity__c",
  legacy_project_id AS "Legacy_Project_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM project_data