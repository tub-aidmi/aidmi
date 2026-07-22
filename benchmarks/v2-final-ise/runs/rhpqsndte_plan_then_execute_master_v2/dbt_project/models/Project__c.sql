{{ config(materialized='table') }}

WITH project_status_mapping AS (
  SELECT 
    LOWER(TRIM(projektstatus)) AS source_status,
    CASE 
      WHEN LOWER(TRIM(projektstatus)) IN ('active') THEN 'Active'
      WHEN LOWER(TRIM(projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
      WHEN LOWER(TRIM(projektstatus)) IN ('in planung', 'planung', 'in planning') THEN 'In Planning'
      WHEN LOWER(TRIM(projektstatus)) IN ('on hold') THEN 'On Hold'
      WHEN LOWER(TRIM(projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
      ELSE NULL
    END AS mapped_status
  FROM (VALUES ('active'), ('Abgeschlossen'), ('In Planung'), ('On Hold'), ('Cancelled'), ('Planung')) AS t(projektstatus)
),

account_ids AS (
  SELECT 
    k.kundennummer,
    MD5(k.kundennummer) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k
),

opportunity_ids AS (
  SELECT 
    o.opp_kennung,
    MD5(o.opp_kennung) AS opportunity_id
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
)

SELECT 
  MD5(p.projekt_kennung) AS "Id",
  COALESCE(NULLIF(INITCAP(TRIM(p.projektname)), ''), 'Untitled Project') AS "Name",
  m.mapped_status AS "Project_Status__c",
  CASE 
    WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND p.go_live_datum != '0000-00-00' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
    WHEN p.go_live_datum ~ '^\d{8}$' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Go_Live_Date__c",
  a.account_id AS "Account__c",
  o.opportunity_id AS "Opportunity__c",
  p.projekt_kennung AS "Legacy_Project_ID__c",
  TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN account_ids a ON p.kunden_kennung = a.kundennummer
LEFT JOIN opportunity_ids o ON p.opp_kennung_ref = o.opp_kennung
LEFT JOIN project_status_mapping m ON LOWER(TRIM(p.projektstatus)) = m.source_status