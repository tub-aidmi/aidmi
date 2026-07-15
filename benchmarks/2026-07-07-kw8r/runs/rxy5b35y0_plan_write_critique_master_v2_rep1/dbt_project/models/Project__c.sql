{{ config(materialized='table') }}
WITH project_data AS (
  SELECT 
    p.projekt_kennung,
    INITCAP(TRIM(p.projektname)) AS project_name,
    p.projektstatus,
    p.go_live_datum,
    p.kunden_kennung,
    p.opp_kennung_ref
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
),
account_mapping AS (
  SELECT 
    k.kundennummer,
    MD5(k.kundennummer) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k
),
opportunity_mapping AS (
  SELECT 
    o.opp_kennung,
    MD5(o.opp_kennung) AS opportunity_id
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }} o
),
asset_account_bridge AS (
  SELECT DISTINCT
    a.projekt_kennung,
    a.kunden_kennung AS asset_kunden_kennung
  FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
)
SELECT 
  MD5(pd.projekt_kennung) AS "Id",
  pd.project_name AS "Name",
  CASE 
    WHEN UPPER(TRIM(pd.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
    WHEN UPPER(TRIM(pd.projektstatus)) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
    WHEN UPPER(TRIM(pd.projektstatus)) IN ('IN PLANUNG', 'PLANUNG', 'IN PLANNING') THEN 'In Planning'
    WHEN UPPER(TRIM(pd.projektstatus)) IN ('ON HOLD', 'ONHOLD', 'PAUSIERT') THEN 'On Hold'
    WHEN UPPER(TRIM(pd.projektstatus)) IN ('CANCELLED', 'STORNIERT') THEN 'Cancelled'
    ELSE NULL 
  END AS "Project_Status__c",
  CASE 
    WHEN pd.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(pd.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
    WHEN pd.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(pd.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN pd.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(pd.go_live_datum, 'M/D/YYYY'), 'YYYY-MM-DD')
    WHEN pd.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(pd.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN pd.go_live_datum = '0000-00-00' THEN NULL
    ELSE NULL 
  END AS "Go_Live_Date__c",
  COALESCE(
    am.account_id,
    am_bridge.account_id
  ) AS "Account__c",
  om.opportunity_id AS "Opportunity__c",
  pd.projekt_kennung AS "Legacy_Project_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM project_data pd
LEFT JOIN account_mapping am ON pd.kunden_kennung = am.kundennummer
LEFT JOIN asset_account_bridge aab ON pd.projekt_kennung = aab.projekt_kennung
LEFT JOIN account_mapping am_bridge ON aab.asset_kunden_kennung = am_bridge.kundennummer
LEFT JOIN opportunity_mapping om ON pd.opp_kennung_ref = om.opp_kennung