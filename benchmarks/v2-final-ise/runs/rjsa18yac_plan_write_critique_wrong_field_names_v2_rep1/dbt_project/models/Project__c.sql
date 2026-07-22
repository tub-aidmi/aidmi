{{ config(materialized='table') }}

WITH proj_data AS (
  SELECT
    p."proj_id",
    TRIM(p."name") AS "name",
    p."status",
    p."go_live",
    p."kd",
    p."opp"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
),
account_mapping AS (
  SELECT
    k."kunden_nr",
    k."kunden_nr" AS "account_id_placeholder"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
),
opportunity_mapping AS (
  SELECT
    c."chance_id",
    c."chance_id" AS "opportunity_id_placeholder"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
)

SELECT
  MD5(p."proj_id" || 'PROJECT') AS "Id",
  COALESCE(NULLIF(TRIM(p."name"), ''), 'Unnamed Project') AS "Name",
  CASE 
    WHEN UPPER(TRIM(p."status")) = 'ACTIVE' THEN 'Active'
    WHEN UPPER(TRIM(p."status")) = 'COMPLETED' THEN 'Completed'
    WHEN UPPER(TRIM(p."status")) = 'IN PLANNING' THEN 'In Planning'
    WHEN UPPER(TRIM(p."status")) = 'ON HOLD' THEN 'On Hold'
    WHEN UPPER(TRIM(p."status")) = 'CANCELLED' THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE 
    WHEN p."go_live" ~ '^\d{4}-\d{2}-\d{2}$' THEN p."go_live"
    ELSE NULL
  END AS "Go_Live_Date__c",
  a."account_id_placeholder" AS "Account__c",
  o."opportunity_id_placeholder" AS "Opportunity__c",
  p."proj_id" AS "Legacy_Project_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM proj_data p
LEFT JOIN account_mapping a ON p."kd" = a."kunden_nr"
LEFT JOIN opportunity_mapping o ON p."opp" = o."chance_id"