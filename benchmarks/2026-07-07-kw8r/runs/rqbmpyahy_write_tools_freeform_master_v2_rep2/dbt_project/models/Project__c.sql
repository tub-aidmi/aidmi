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
    kundennummer,
    '001' || SUBSTRING(MD5(kundennummer) FROM 1 FOR 15) AS account_id
  FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

opportunity_mapping AS (
  SELECT 
    opp_kennung,
    '006' || SUBSTRING(MD5(opp_kennung) FROM 1 FOR 15) AS opportunity_id
  FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)

SELECT
  -- Generate deterministic Salesforce-style ID from natural key
  '009' || SUBSTRING(MD5(p.projekt_kennung) FROM 1 FOR 15) AS "Id",
  
  -- Name (required)
  COALESCE(NULLIF(TRIM(p.projektname), ''), 'Project ' || p.projekt_kennung) AS "Name",
  
  -- Project Status: map to enum values
  CASE 
    WHEN UPPER(TRIM(p.projektstatus)) IN ('ACTIVE', 'AKTIV') THEN 'Active'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('COMPLETED', 'ABGESCHLOSSEN') THEN 'Completed'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('IN PLANNING', 'IN PLANUNG', 'PLANUNG') THEN 'In Planning'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('ON HOLD', 'ON HOLD') THEN 'On Hold'
    WHEN UPPER(TRIM(p.projektstatus)) IN ('CANCELLED', 'CANCELLED', 'STORNIERT') THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  
  -- Go Live Date: parse various date formats
  CASE 
    WHEN p.go_live_datum IS NULL OR p.go_live_datum IN ('N/A', 'None', '') THEN NULL
    WHEN p.go_live_datum = '0000-00-00' THEN NULL
    WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN 
      CASE 
        WHEN CAST(SUBSTRING(p.go_live_datum FROM 6 FOR 2) AS INTEGER) BETWEEN 1 AND 12 AND
             CAST(SUBSTRING(p.go_live_datum FROM 9 FOR 2) AS INTEGER) BETWEEN 1 AND 31
        THEN p.go_live_datum
        ELSE NULL
      END
    WHEN p.go_live_datum ~ '^\d{8}$' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YY'), 'YYYY-MM-DD')
    WHEN p.go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN p.go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{2}$' THEN 
      TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YY'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Go_Live_Date__c",
  
  -- Account__c: join to customer
  CASE 
    WHEN p.kunden_kennung LIKE 'CUST-M%' THEN am.account_id
    ELSE NULL
  END AS "Account__c",
  
  -- Opportunity__c: join to opportunity
  CASE 
    WHEN p.opp_kennung_ref LIKE 'OPP-%' THEN om.opportunity_id
    ELSE NULL
  END AS "Opportunity__c",
  
  -- Legacy Project ID from source natural key
  p.projekt_kennung AS "Legacy_Project_ID__c",
  
  -- Timestamps
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "LastModifiedDate",
  
  -- Not deleted
  0 AS "IsDeleted"

FROM project_data p
LEFT JOIN account_mapping am ON p.kunden_kennung = am.kundennummer
LEFT JOIN opportunity_mapping om ON p.opp_kennung_ref = om.opp_kennung
