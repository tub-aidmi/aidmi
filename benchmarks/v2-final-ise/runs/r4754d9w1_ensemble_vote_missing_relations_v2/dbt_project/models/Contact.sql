{{ config(materialized='table') }}

WITH contact_with_names AS (
  SELECT
    id,
    TRIM(SPLIT_PART(full_name, ' ', 1)) AS first_name,
    TRIM(
      SUBSTRING(
        full_name,
        GREATEST(1, POSITION(' ' IN REVERSE(full_name)) + 1)
      )
    ) AS last_name,
    email,
    account_ref,
    company_name
  FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
account_mapping AS (
  SELECT
    c.id AS contact_id,
    c.first_name,
    c.last_name,
    c.email,
    c.account_ref,
    c.company_name,
    a.id AS account_id
  FROM contact_with_names c
  LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c.account_ref = a.id
),
account_fallback AS (
  SELECT
    contact_id,
    first_name,
    last_name,
    email,
    account_ref,
    company_name,
    COALESCE(account_id, a.id) AS account_id
  FROM account_mapping
  LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON account_mapping.account_id IS NULL
    AND account_mapping.company_name = a.name
)

SELECT
  contact_id AS "Id",
  first_name AS "FirstName",
  last_name AS "LastName",
  email AS "Email",
  NULL::text AS "Phone",
  NULL::text AS "Title",
  NULL::text AS "Role__c",
  NULL::text AS "Preferred_Language__c",
  account_id AS "AccountId",
  contact_id AS "Legacy_Contact_ID__c",
  NULL::text AS "CreatedDate",
  NULL::text AS "LastModifiedDate",
  0 AS "IsDeleted"
FROM account_fallback