{{ config(materialized='table') }}

SELECT
  id AS "Id",
  NULLIF(TRIM(SPLIT_PART(full_name, ' ', 1)), '') AS "FirstName",
  CASE 
    WHEN full_name IS NULL THEN 'Unknown'
    WHEN POSITION(' ' IN full_name) = 0 THEN 'Unknown'
    ELSE TRIM(SUBSTR(full_name, POSITION(' ' IN full_name) + 1))
  END AS "LastName",
  email AS "Email",
  NULL::TEXT AS "Phone",
  NULL::TEXT AS "Title",
  CASE 
    WHEN LOWER(TRIM(company_name)) IS NOT NULL THEN INITCAP(LOWER(TRIM(company_name)))
    ELSE NULL
  END AS "Role__c",
  NULL::TEXT AS "Preferred_Language__c",
  TRIM(UPPER(account_ref)) AS "AccountId",
  id AS "Legacy_Contact_ID__c",
  NULL::TEXT AS "CreatedDate",
  NULL::TEXT AS "LastModifiedDate",
  0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}