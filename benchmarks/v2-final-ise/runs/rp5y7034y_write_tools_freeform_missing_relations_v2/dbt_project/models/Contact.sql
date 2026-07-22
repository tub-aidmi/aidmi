{{ config(materialized='table') }}

SELECT
  CAST(c."id" AS TEXT) AS "Id",
  CASE
    WHEN POSITION(' ' IN c."full_name") > 0 THEN TRIM(REGEXP_REPLACE(c."full_name", '\s\S+$', ''))
    ELSE NULL
  END AS "FirstName",
  CASE
    WHEN POSITION(' ' IN c."full_name") > 0 THEN SPLIT_PART(c."full_name", ' ', -1)
    ELSE c."full_name"
  END AS "LastName",
  c."email" AS "Email",
  NULL AS "Phone",
  NULL AS "Title",
  NULL AS "Role__c",
  NULL AS "Preferred_Language__c",
  CASE
    WHEN c."account_ref" IS NOT NULL THEN c."account_ref"
    WHEN c."company_name" IS NOT NULL THEN (
      SELECT a."id"
      FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
      WHERE a."name" = c."company_name"
      LIMIT 1
    )
    ELSE NULL
  END AS "AccountId",
  CAST(c."id" AS TEXT) AS "Legacy_Contact_ID__c",
  NULL AS "CreatedDate",
  NULL AS "LastModifiedDate",
  0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
