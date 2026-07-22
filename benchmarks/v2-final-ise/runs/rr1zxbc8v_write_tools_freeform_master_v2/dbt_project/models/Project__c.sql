{{ config(materialized='table') }}

WITH project_data AS (
  SELECT
    projekt_kennung,
    projektname,
    projektstatus,
    go_live_datum,
    kunden_kennung,
    opp_kennung_ref
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
),

account_mapping AS (
  SELECT
    kundennummer AS legacy_customer_id,
    SUBSTRING(MD5('Account_' || kundennummer) FROM 1 FOR 18) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

opportunity_mapping AS (
  SELECT
    opp_kennung AS legacy_opportunity_id,
    SUBSTRING(MD5('Opportunity_' || opp_kennung) FROM 1 FOR 18) AS opportunity_id
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
),

-- Parse dates from various formats
parsed_dates AS (
  SELECT
    projekt_kennung,
    projektname,
    projektstatus,
    kunden_kennung,
    opp_kennung_ref,
    -- Parse go_live_datum from various formats
    CASE 
      WHEN go_live_datum IS NULL OR TRIM(go_live_datum) IN ('', 'N/A', '0000-00-00') THEN NULL
      WHEN go_live_datum ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 
        TO_CHAR(TO_DATE(go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN 
        TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN 
        TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^[0-9]{8}$' THEN 
        TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^[0-9]{4}[0-9]{2}[0-9]{2}$' THEN 
        TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
      ELSE NULL
    END AS go_live_date
  FROM project_data
)

SELECT
  -- Generate deterministic Salesforce-style Id
  SUBSTRING(MD5('Project_' || p.projekt_kennung) FROM 1 FOR 18) AS "Id",
  
  -- Name: use projektname
  COALESCE(NULLIF(TRIM(p.projektname), ''), p.projekt_kennung) AS "Name",
  
  -- Project Status: normalize to enum values
  CASE 
    WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'PAUSIERT') THEN 'On Hold'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'STORNIERT', 'CANCELLED') THEN 'Cancelled'
    ELSE 'In Planning'
  END AS "Project_Status__c",
  
  -- Go Live Date
  p.go_live_date AS "Go_Live_Date__c",
  
  -- Account__c: lookup from master_kunden via kunden_kennung
  am.account_id AS "Account__c",
  
  -- Opportunity__c: lookup from master_opportunities via opp_kennung_ref
  om.opportunity_id AS "Opportunity__c",
  
  -- Legacy Project ID
  p.projekt_kennung AS "Legacy_Project_ID__c",
  
  -- CreatedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  
  -- LastModifiedDate
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- IsDeleted: default to 0
  0 AS "IsDeleted"

FROM parsed_dates p
LEFT JOIN account_mapping am ON p.kunden_kennung = am.legacy_customer_id
LEFT JOIN opportunity_mapping om ON REPLACE(p.opp_kennung_ref, 'OPP-M-', 'OPP-') = om.legacy_opportunity_id
