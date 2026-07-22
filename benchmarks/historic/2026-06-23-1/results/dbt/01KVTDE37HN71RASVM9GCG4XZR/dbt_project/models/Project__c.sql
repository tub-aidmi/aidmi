{{ config(materialized='table') }}

WITH date_parsed AS (
  SELECT
    projekt_kennung,
    projektname,
    projektstatus,
    CASE
      WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
      WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS go_live_datum_iso,
    kunden_kennung,
    opp_kennung_ref
  FROM {{ source('fixture_master_src', 'master_projekte') }}
),
status_mapped AS (
  SELECT
    projekt_kennung,
    projektname,
    CASE
      WHEN UPPER(TRIM(projektstatus)) = 'ACTIVE' THEN 'Active'
      WHEN UPPER(TRIM(projektstatus)) IN ('IN BEARBEITUNG', 'IN PLANNING') THEN 'In Planning'
      WHEN UPPER(TRIM(projektstatus)) IN ('INACTIVE', 'ON HOLD') THEN 'On Hold'
      WHEN UPPER(TRIM(projektstatus)) = 'COMPLETED' THEN 'Completed'
      WHEN UPPER(TRIM(projektstatus)) = 'CANCELLED' THEN 'Cancelled'
      ELSE 'In Planning'
    END AS project_status__c,
    go_live_datum_iso,
    kunden_kennung,
    opp_kennung_ref
  FROM date_parsed
)

SELECT
  projekt_kennung AS Id,
  INITCAP(TRIM(projektname)) AS Name,
  project_status__c AS Project_Status__c,
  go_live_datum_iso AS Go_Live_Date__c,
  kunden_kennung AS Account__c,
  opp_kennung_ref AS Opportunity__c,
  projekt_kennung AS Legacy_Project_ID__c,
  CURRENT_TIMESTAMP::text AS CreatedDate,
  CURRENT_TIMESTAMP::text AS LastModifiedDate,
  0 AS IsDeleted
FROM status_mapped