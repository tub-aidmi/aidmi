{{ config(materialized='table') }}

SELECT
  COALESCE(UPPER(TRIM(p.id)), 'UNKNOWN') AS "Id",
  COALESCE(INITCAP(TRIM(p.name)), 'Unknown') AS "Name",
  CASE INITCAP(TRIM(p.status))
    WHEN 'Active' THEN 'Active'
    WHEN 'Completed' THEN 'Completed'
    WHEN 'In Planning' THEN 'In Planning'
    WHEN 'On Hold' THEN 'On Hold'
    WHEN 'Cancelled' THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE 
    WHEN p.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
    WHEN p.go_live ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN p.go_live ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
    ELSE NULL
  END AS "Go_Live_Date__c",
  UPPER(TRIM(p.client_id)) AS "Account__c",
  UPPER(TRIM(p.opportunity_ref)) AS "Opportunity__c",
  TRIM(p.id) AS "Legacy_Project_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p