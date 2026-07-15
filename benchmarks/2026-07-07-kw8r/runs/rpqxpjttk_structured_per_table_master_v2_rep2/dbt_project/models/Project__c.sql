{{ config(materialized='table') }}

WITH date_parsed AS (
  SELECT
    projekt_kennung,
    go_live_datum,
    CASE
      WHEN go_live_datum = '0000-00-00' THEN NULL
      WHEN go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live_datum
      WHEN go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
      WHEN go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      ELSE NULL
    END AS parsed_go_live_date
  FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
  '001' || mp.projekt_kennung AS "Id",
  mp.projektname AS "Name",
  CASE
    WHEN mp.projektstatus IN ('Abgeschlossen', 'Completed', 'completed') THEN 'Completed'
    WHEN mp.projektstatus IN ('Active', 'Aktiv', 'active') THEN 'Active'
    WHEN mp.projektstatus IN ('In Planning', 'In Planung', 'Planung') THEN 'In Planning'
    WHEN mp.projektstatus IN ('On Hold', 'on hold', 'Pausiert') THEN 'On Hold'
    WHEN mp.projektstatus IN ('Cancelled', 'cancelled', 'Storniert') THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  dp.parsed_go_live_date AS "Go_Live_Date__c",
  CASE WHEN mk.kundennummer IS NOT NULL THEN '001' || mk.kundennummer ELSE NULL END AS "Account__c",
  CASE WHEN mo.opp_kennung IS NOT NULL THEN '001' || mo.opp_kennung ELSE NULL END AS "Opportunity__c",
  mp.projekt_kennung AS "Legacy_Project_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
LEFT JOIN date_parsed dp ON mp.projekt_kennung = dp.projekt_kennung
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk ON mp.kunden_kennung = mk.kundennummer
LEFT JOIN {{ source('fixture_master_v2_src', 'master_opportunities') }} mo ON mp.opp_kennung_ref = mo.opp_kennung