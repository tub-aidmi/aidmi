{{ config(materialized='table') }}
SELECT 
  'PROJ-' || mp.projekt_kennung AS "Id",
  mp.projektname AS "Name",
  CASE 
    WHEN UPPER(TRIM(mp.projektstatus)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
    WHEN UPPER(TRIM(mp.projektstatus)) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
    WHEN UPPER(TRIM(mp.projektstatus)) IN ('IN PLANUNG', 'PLANNING') THEN 'In Planning'
    WHEN UPPER(TRIM(mp.projektstatus)) IN ('PAUSIERT', 'ON HOLD') THEN 'On Hold'
    WHEN UPPER(TRIM(mp.projektstatus)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
    ELSE NULL 
  END AS "Project_Status__c",
  CASE 
    WHEN mp.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN mp.go_live_datum
    WHEN mp.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN mp.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(mp.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    ELSE NULL 
  END AS "Go_Live_Date__c",
  'ACC-' || mk.kundennummer AS "Account__c",
  CASE 
    WHEN mp.opp_kennung_ref IS NOT NULL THEN 'OPP-' || mp.opp_kennung_ref 
    ELSE NULL 
  END AS "Opportunity__c",
  mp.projekt_kennung AS "Legacy_Project_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk 
  ON mp.kunden_kennung = mk.kundennummer