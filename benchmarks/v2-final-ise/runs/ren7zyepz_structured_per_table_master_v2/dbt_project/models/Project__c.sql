{{ config(materialized='table') }}
WITH project_status_mapping AS (
  SELECT
    projekt_kennung,
    CASE
      WHEN LOWER(TRIM(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
      WHEN LOWER(TRIM(projektstatus)) IN ('abgeschlossen', 'completed') THEN 'Completed'
      WHEN LOWER(TRIM(projektstatus)) IN ('in planung', 'in planning', 'planung') THEN 'In Planning'
      WHEN LOWER(TRIM(projektstatus)) IN ('on hold', 'pausiert') THEN 'On Hold'
      WHEN LOWER(TRIM(projektstatus)) IN ('cancelled', 'storniert') THEN 'Cancelled'
      ELSE NULL
    END AS mapped_status
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
),
parsed_dates AS (
  SELECT
    projekt_kennung,
    CASE
      WHEN go_live_datum = '0000-00-00' THEN NULL
      WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
        TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN
        CASE WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' AND go_live_datum != '0000-00-00' THEN go_live_datum ELSE NULL END
      WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
        TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^\d{8}$' THEN
        TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
      ELSE NULL
    END AS parsed_go_live_date
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)
SELECT
  p.projekt_kennung AS "Id",
  p.projektname AS "Name",
  pm.mapped_status AS "Project_Status__c",
  pd.parsed_go_live_date AS "Go_Live_Date__c",
  p.kunden_kennung AS "Account__c",
  p.opp_kennung_ref AS "Opportunity__c",
  p.projekt_kennung AS "Legacy_Project_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
LEFT JOIN project_status_mapping pm ON p.projekt_kennung = pm.projekt_kennung
LEFT JOIN parsed_dates pd ON p.projekt_kennung = pd.projekt_kennung