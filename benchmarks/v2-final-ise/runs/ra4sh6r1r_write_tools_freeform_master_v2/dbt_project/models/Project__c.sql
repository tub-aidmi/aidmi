{{ config(materialized='table') }}

WITH project_data AS (
  SELECT 
    '' || MD5(COALESCE(p.projekt_kennung, '') || COALESCE(p.projektname, '')) AS project_id,
    
    -- Join to customer to get Account__c
    '' || MD5(COALESCE(kd.kundennummer, '') || COALESCE(kd.unternehmensname, '')) AS account_id,
    
    -- Join to opportunity to get Opportunity__c
    CASE 
      WHEN o.opp_kennung IS NOT NULL THEN 
        '' || MD5(COALESCE(o.opp_kennung, '') || COALESCE(o.titel, ''))
      ELSE NULL
    END AS opportunity_id,
    
    COALESCE(NULLIF(TRIM(p.projektname), ''), 'Untitled Project') AS project_name,
    
    -- Map projektstatus to Project_Status__c
    CASE 
      WHEN UPPER(TRIM(p.projektstatus)) IN ('AKTIV', 'ACTIVE', 'AKTIV') THEN 'Active'
      WHEN UPPER(TRIM(p.projektstatus)) IN ('ABGESCHLOSSEN', 'COMPLETED', 'FERTIG') THEN 'Completed'
      WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANUNG', 'IN PLANNING', 'PLANUNG') THEN 'In Planning'
      WHEN UPPER(TRIM(p.projektstatus)) IN ('AUF EIS', 'ON HOLD', 'PAUSIERT') THEN 'On Hold'
      WHEN UPPER(TRIM(p.projektstatus)) IN ('STORNIERT', 'CANCELLED', 'ABGEBROCHEN') THEN 'Cancelled'
      ELSE NULL
    END AS project_status,
    
    -- Parse go_live_datum
    CASE 
      WHEN TRIM(p.go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(p.go_live_datum)
      WHEN TRIM(p.go_live_datum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
        TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN TRIM(p.go_live_datum) ~ '^\d{4}\d{2}\d{2}$' THEN 
        TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN TRIM(p.go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
        TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS go_live_date,
    
    TRIM(p.projekt_kennung) AS legacy_project_id,
    '2024-01-01' AS created_date,
    '2024-01-01' AS last_modified_date,
    0 AS is_deleted
    
  FROM {{ source(source_slug, 'master_projekte') }} p
  LEFT JOIN {{ source(source_slug, 'master_kunden') }} kd 
    ON TRIM(p.kunden_kennung) = TRIM(kd.kundennummer)
  LEFT JOIN {{ source(source_slug, 'master_opportunities') }} o 
    ON TRIM(p.opp_kennung_ref) = TRIM(o.opp_kennung)
)

SELECT 
  project_id AS "Id",
  project_name AS "Name",
  project_status AS "Project_Status__c",
  go_live_date AS "Go_Live_Date__c",
  account_id AS "Account__c",
  opportunity_id AS "Opportunity__c",
  legacy_project_id AS "Legacy_Project_ID__c",
  created_date AS "CreatedDate",
  last_modified_date AS "LastModifiedDate",
  is_deleted AS "IsDeleted"

FROM project_data
