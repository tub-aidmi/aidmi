{{
  config(
    materialized='table'
  )
}}

SELECT
  MD5(master_projekte.projekt_kennung) AS "Id",
  COALESCE(master_projekte.projektname, 'Unknown Project') AS "Name",
  CASE
    WHEN LOWER(master_projekte.projektstatus) = 'active' THEN 'Active'
    WHEN LOWER(master_projekte.projektstatus) = 'completed' THEN 'Completed'
    WHEN LOWER(master_projekte.projektstatus) = 'in planning' THEN 'In Planning'
    WHEN LOWER(master_projekte.projektstatus) = 'on hold' THEN 'On Hold'
    WHEN LOWER(master_projekte.projektstatus) = 'cancelled' THEN 'Cancelled'
    ELSE 'In Planning' -- Default for NOT NULL Project_Status__c
  END AS "Project_Status__c",
  COALESCE(
    TO_CHAR(TO_DATE(master_projekte.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
    TO_CHAR(TO_DATE(master_projekte.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
    TO_CHAR(TO_DATE(master_projekte.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL Go_Live_Date__c
  ) AS "Go_Live_Date__c",
  MD5(master_projekte.kunden_kennung) AS "Account__c", -- Assuming kunden_kennung links to master_kunden.kundennummer
  MD5(master_projekte.opp_kennung_ref) AS "Opportunity__c", -- Assuming opp_kennung_ref links to master_opportunities.opp_kennung
  master_projekte.projekt_kennung AS "Legacy_Project_ID__c",
  NOW()::TEXT AS "CreatedDate",
  NOW()::TEXT AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM
  {{ source('fixture_master_v2_src', 'master_projekte') }} AS master_projekte
