{{ config(materialized='table') }}
WITH project_data AS (
  SELECT
    p.projekt_kennung,
    p.projektname,
    p.projektstatus,
    p.go_live_datum,
    p.kunden_kennung,
    p.opp_kennung_ref,
    k.kundennummer AS account_legacy_id,
    o.opp_kennung AS opportunity_legacy_id
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
  LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
    ON p.kunden_kennung = k.kundennummer
  LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} o
    ON p.opp_kennung_ref = o.opp_kennung
)
SELECT
  gen_random_uuid()::text AS "Id",
  TRIM(p.projektname) AS "Name",
  CASE
    WHEN TRIM(LOWER(p.projektstatus)) IN ('aktiv', 'active') THEN 'Active'
    WHEN TRIM(LOWER(p.projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
    WHEN TRIM(LOWER(p.projektstatus)) IN ('in planung', 'in planning') THEN 'In Planning'
    WHEN TRIM(LOWER(p.projektstatus)) IN ('pausiert', 'on hold') THEN 'On Hold'
    WHEN TRIM(LOWER(p.projektstatus)) IN ('storniert', 'cancelled') THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE
    WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live_datum
    WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Go_Live_Date__c",
  CASE WHEN p.account_legacy_id IS NOT NULL THEN MD5(p.account_legacy_id) ELSE NULL END AS "Account__c",
  p.opportunity_legacy_id AS "Opportunity__c",
  p.projekt_kennung AS "Legacy_Project_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM project_data p