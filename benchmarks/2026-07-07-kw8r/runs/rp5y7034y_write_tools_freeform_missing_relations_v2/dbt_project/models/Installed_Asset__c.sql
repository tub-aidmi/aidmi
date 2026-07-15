{{ config(materialized='table') }}

SELECT
  CAST(a."id" AS TEXT) AS "Id",
  a."name" AS "Name",
  a."serial" AS "Serial_Number__c",
  CAST(a."warranty" AS TEXT) AS "Warranty_End_Date__c",
  CASE
    WHEN a."client" IS NOT NULL AND a."client" ~ '^ACC-\d+$' THEN a."client"
    WHEN a."client" IS NOT NULL THEN (
      SELECT ac."id"
      FROM {{ source('fixture_missing_relations_v2_src', 'account') }} ac
      WHERE ac."name" = a."client"
      LIMIT 1
    )
    ELSE NULL
  END AS "Account__c",
  a."project" AS "Project__c",
  CAST(a."id" AS TEXT) AS "Legacy_Asset_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
