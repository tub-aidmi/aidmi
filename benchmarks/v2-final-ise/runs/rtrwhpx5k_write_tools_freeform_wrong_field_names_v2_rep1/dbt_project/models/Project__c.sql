{{ config(materialized='table') }}

WITH proj_data AS (
  SELECT
    proj_id,
    name,
    status,
    go_live,
    kd,
    opp
  FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
),

account_mapping AS (
  SELECT
    kunden_nr AS "AccountId",
    kunden_nr AS "Legacy_Customer_ID__c"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

opportunity_mapping AS (
  SELECT
    chance_id AS "OpportunityId",
    chance_id AS "Legacy_Opportunity_ID__c"
  FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

SELECT
  proj_id AS "Id",
  name AS "Name",
  CASE
    WHEN UPPER(TRIM(status)) IN ('AKTIV', 'ACTIVE') THEN 'Active'
    WHEN UPPER(TRIM(status)) IN ('ABGESCHLOSSEN', 'COMPLETED') THEN 'Completed'
    WHEN UPPER(TRIM(status)) IN ('IN PLANUNG', 'IN PLANNING') THEN 'In Planning'
    WHEN UPPER(TRIM(status)) IN ('IN BEARBEITUNG', 'ON HOLD') THEN 'On Hold'
    WHEN UPPER(TRIM(status)) IN ('STORNIERT', 'CANCELLED') THEN 'Cancelled'
    ELSE NULL
  END AS "Project_Status__c",
  CASE
    WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
    WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
    WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
    WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
    ELSE NULL
  END AS "Go_Live_Date__c",
  account_mapping."AccountId" AS "Account__c",
  opportunity_mapping."OpportunityId" AS "Opportunity__c",
  proj_id AS "Legacy_Project_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM proj_data
LEFT JOIN account_mapping ON proj_data.kd = account_mapping."Legacy_Customer_ID__c"
LEFT JOIN opportunity_mapping ON proj_data.opp = opportunity_mapping."Legacy_Opportunity_ID__c"
