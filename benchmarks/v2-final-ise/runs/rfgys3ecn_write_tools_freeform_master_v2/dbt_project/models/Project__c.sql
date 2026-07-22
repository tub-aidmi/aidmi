{{ config(materialized='table') }}
WITH source_data AS (
  SELECT
    projekt_kennung,
    projektname,
    projektstatus,
    go_live_datum,
    kunden_kennung,
    opp_kennung_ref
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)
SELECT
  gen_random_uuid()::text AS "Id",
  INITCAP(TRIM(projektname)) AS "Name",
  CASE
    WHEN UPPER(TRIM(projektstatus)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
    WHEN UPPER(TRIM(projektstatus)) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
    WHEN UPPER(TRIM(projektstatus)) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
    WHEN UPPER(TRIM(projektstatus)) IN ('AUF EIS', 'ON HOLD') THEN 'On Hold'
    WHEN UPPER(TRIM(projektstatus)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE
    WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
    WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Go_Live_Date__c",
  CASE WHEN TRIM(kunden_kennung) IS NOT NULL THEN md5('ns:' || TRIM(kunden_kennung)) ELSE NULL END AS "Account__c",
  CASE WHEN TRIM(opp_kennung_ref) IS NOT NULL THEN md5('ns:' || TRIM(opp_kennung_ref)) ELSE NULL END AS "Opportunity__c",
  TRIM(projekt_kennung) AS "Legacy_Project_ID__c",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM source_data