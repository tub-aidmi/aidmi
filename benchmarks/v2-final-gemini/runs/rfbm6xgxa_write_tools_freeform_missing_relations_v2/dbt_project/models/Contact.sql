{{
  config(
    materialized='table'
  )
}}

SELECT
    id AS "Id",
    SPLIT_PART(full_name, ' ', 1) AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SUBSTRING(full_name, POSITION(' ' IN full_name) + 1)), ''),
        full_name,
        'Unknown'
    ) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c", -- No direct mapping, default to NULL
    NULL AS "Preferred_Language__c", -- No direct mapping, default to NULL
    account_ref AS "AccountId", -- Assuming account_ref directly maps to Account.Id
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }}
