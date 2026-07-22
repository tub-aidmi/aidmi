{{ config(materialized='table') }}

SELECT
  CAST(p."id" AS TEXT) AS "Id",
  p."name" AS "Name",
  CASE
    WHEN UPPER(TRIM(p."status")) = 'ACTIVE' THEN 'Active'
    WHEN UPPER(TRIM(p."status")) = 'COMPLETED' THEN 'Completed'
    WHEN UPPER(TRIM(p."status")) = 'IN PLANNING' THEN 'In Planning'
    WHEN UPPER(TRIM(p."status")) = 'ON HOLD' THEN 'On Hold'
    WHEN UPPER(TRIM(p."status")) = 'CANCELLED' THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CAST(p."go_live" AS TEXT) AS "Go_Live_Date__c",
  p."client_id" AS "Account__c",
  p."opportunity_ref" AS "Opportunity__c",
  CAST(p."id" AS TEXT) AS "Legacy_Project_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p
