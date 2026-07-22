{{ config(materialized='table') }}
WITH project_data AS (
  SELECT
    p.projekt_kennung,
    TRIM(p.projektname) AS name,
    p.projektstatus AS status,
    p.go_live_datum AS go_live_date,
    p.kunden_kennung AS customer_id,
    p.opp_kennung_ref AS opportunity_ref
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
),
normalized_projects AS (
  SELECT
    projekt_kennung,
    name,
    -- Normalize project status to enum
    CASE
      WHEN UPPER(TRIM(status)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
      WHEN UPPER(TRIM(status)) IN ('COMPLETED', 'ABGESCHLOSSEN', 'FERTIG') THEN 'Completed'
      WHEN UPPER(TRIM(status)) IN ('IN PLANNING', 'IN PLANUNG') THEN 'In Planning'
      WHEN UPPER(TRIM(status)) IN ('ON HOLD', 'ANGEHALTEN') THEN 'On Hold'
      WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
      ELSE NULL
    END AS status,
    -- Parse go_live_date: handle YYYY-MM-DD, MM/DD/YYYY, 0000-00-00
    CASE
      WHEN go_live_date IS NULL OR TRIM(go_live_date) = '' THEN NULL
      WHEN TRIM(go_live_date) = '0000-00-00' THEN NULL
      WHEN TRIM(go_live_date) ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CASE WHEN TO_DATE(TRIM(go_live_date), 'YYYY-MM-DD') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(go_live_date), 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END
      WHEN TRIM(go_live_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
        CASE WHEN TO_DATE(TRIM(go_live_date), 'MM/DD/YYYY') IS NOT NULL THEN TO_CHAR(TO_DATE(TRIM(go_live_date), 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END
      ELSE NULL
    END AS go_live_date,
    customer_id,
    opportunity_ref
  FROM project_data
)
SELECT
  projekt_kennung AS "Id",
  COALESCE(NULLIF(name, ''), 'Untitled Project') AS "Name",
  COALESCE(status, 'In Planning') AS "Project_Status__c",
  go_live_date AS "Go_Live_Date__c",
  customer_id AS "Account__c",
  -- Map opportunity_ref to Opportunity Id
  CASE
    WHEN opportunity_ref IS NULL THEN NULL
    WHEN opportunity_ref LIKE 'OPP-M-%' THEN REPLACE(opportunity_ref, 'OPP-M-', 'OPP-')
    ELSE opportunity_ref
  END AS "Opportunity__c",
  projekt_kennung AS "Legacy_Project_ID__c",
  CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
  CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM normalized_projects