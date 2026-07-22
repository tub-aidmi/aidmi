{{ config(materialized='table') }}

SELECT
  REGEXP_REPLACE(TRIM(UPPER(projekt_kennung)), '^[A-Z]+', '', 'g') AS "Id",
  COALESCE(NULLIF(TRIM(INITCAP(projektname)), ''), 'Unnamed Project') AS "Name",
  CASE UPPER(TRIM(COALESCE(projektstatus, '')))
    WHEN 'AKTIV' THEN 'Active'
    WHEN 'ABGESCHLOSSEN' THEN 'Completed'
    WHEN 'FERTIG' THEN 'Completed'
    WHEN 'IN PLANUNG' THEN 'In Planning'
    WHEN 'PAUSIERT' THEN 'On Hold'
    WHEN 'ON HOLD' THEN 'On Hold'
    WHEN 'STORNIERT' THEN 'Cancelled'
    WHEN 'GESPERRT' THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE 
    WHEN TRIM(go_live_datum) = '' OR go_live_datum IS NULL THEN NULL
    WHEN go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_datum), 'DD.MM.YYYY')::TEXT
    WHEN go_live_datum ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_datum), 'YYYYMMDD')::TEXT
    ELSE NULL
  END AS "Go_Live_Date__c",
  REGEXP_REPLACE(TRIM(UPPER(kunden_kennung)), '^[A-Z]+', '', 'g') AS "Account__c",
  REGEXP_REPLACE(TRIM(UPPER(opp_kennung_ref)), '^[A-Z]+', '', 'g') AS "Opportunity__c",
  projekt_kennung AS "Legacy_Project_ID__c",
  '2024-01-01 00:00:00' AS "CreatedDate",
  '2024-01-01 00:00:00' AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}