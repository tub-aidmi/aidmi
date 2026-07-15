{{ config(materialized='table') }}

WITH proj_data AS (
  SELECT
    p.proj_id,
    p.name,
    p.status,
    p.go_live,
    p.kd,
    p.opp
  FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
),
account_mapping AS (
  SELECT
    k.kunden_nr,
    k.kunden_nr AS account_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
),
opportunity_mapping AS (
  SELECT
    c.chance_id,
    c.chance_id AS opportunity_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
)

SELECT
  p.proj_id AS "Id",
  p.name AS "Name",
  CASE
    WHEN LOWER(TRIM(p.status)) IN ('active', 'aktiv') THEN 'Active'
    WHEN LOWER(TRIM(p.status)) IN ('completed', 'abgeschlossen') THEN 'Completed'
    WHEN LOWER(TRIM(p.status)) IN ('in planning', 'in planung') THEN 'In Planning'
    WHEN LOWER(TRIM(p.status)) IN ('on hold', 'pausiert') THEN 'On Hold'
    WHEN LOWER(TRIM(p.status)) IN ('cancelled', 'storniert') THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE
    WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
    WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN p.go_live IS NOT NULL AND p.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Go_Live_Date__c",
  a.account_id AS "Account__c",
  o.opportunity_id AS "Opportunity__c",
  p.proj_id AS "Legacy_Project_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM proj_data p
LEFT JOIN account_mapping a ON p.kd = a.kunden_nr
LEFT JOIN opportunity_mapping o ON p.opp = o.chance_id