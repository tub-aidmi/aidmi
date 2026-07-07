-- This dbt model transforms source opportunity data into the target Opportunity schema.

{{ config(materialized='table') }}

SELECT
  o.id AS "Id",
  o.name AS "Name",
  CASE
    WHEN o.stage = 'Prospecting' THEN 'Prospecting'
    WHEN o.stage = 'Qualification' THEN 'Qualification'
    WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
    WHEN o.stage = 'Closed Won' THEN 'Closed Won'
    ELSE 'Prospecting' -- Default to Prospecting if stage is unknown, as StageName is NOT NULL
  END AS "StageName",
  TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS "CloseDate", -- No direct source, using current date as a meaningful default for NOT NULL column
  o.amount AS "Amount",
  NULL::text AS "CurrencyIsoCode", -- No direct source, defaulting to NULL
  a.id AS "AccountId", -- Joined from the account table
  o.id AS "Legacy_Opportunity_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
  ON o.customer_number = a.id