{{ config(materialized='table') }}

SELECT 
  projekt_kennung AS Id,
  INITCAP(TRIM(COALESCE(projektname, 'Unknown'))) AS Name,
  CASE 
    WHEN TRIM(LOWER(projektstatus)) IN ('active', 'aktiv') THEN 'Active'
    WHEN TRIM(LOWER(projektstatus)) = 'pending' THEN 'In Planning'
    WHEN TRIM(projektstatus) = 'In Bearbeitung' THEN 'On Hold'
    WHEN TRIM(LOWER(projektstatus)) IN ('inactive', 'inaktiv') THEN 'Cancelled'
    ELSE NULL
  END AS Project_Status__c,
  CASE 
    WHEN TRIM(go_live_datum) IN ('N/A', '0000-00-00', '') THEN NULL
    ELSE TRIM(go_live_datum)
  END AS Go_Live_Date__c,
  kunden_kennung AS Account__c,
  opp_kennung_ref AS Opportunity__c,
  projekt_kennung AS Legacy_Project_ID__c,
  NULL::text AS CreatedDate,
  NULL::text AS LastModifiedDate,
  0 AS IsDeleted
FROM {{ source('fixture_master_src', 'master_projekte') }}