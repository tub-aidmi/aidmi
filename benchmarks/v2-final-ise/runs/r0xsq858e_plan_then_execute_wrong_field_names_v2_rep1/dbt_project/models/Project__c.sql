{{ config(materialized='table') }}

WITH proj_data AS (
  SELECT
    proj."proj_id",
    TRIM(proj."name") AS "name",
    proj."status",
    proj."go_live",
    proj."kd",
    proj."opp"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj
)

SELECT
  gen_random_uuid()::text AS "Id",
  COALESCE(NULLIF(TRIM(pd."name"), ''), pd."proj_id") AS "Name",
  pd."status" AS "Project_Status__c",
  CASE 
    WHEN pd."go_live" ~ '^\d{4}-\d{2}-\d{2}$' 
    THEN pd."go_live"
    ELSE NULL 
  END AS "Go_Live_Date__c",
  k."kunden_nr" AS "Account__c",
  c."chance_id" AS "Opportunity__c",
  pd."proj_id" AS "Legacy_Project_ID__c",
  CURRENT_TIMESTAMP::text AS "CreatedDate",
  CURRENT_TIMESTAMP::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM proj_data pd
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
  ON pd."kd" = k."kunden_nr"
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
  ON pd."opp" = c."chance_id"