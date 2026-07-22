-- depends_on: {{ source('fixture_missing_relations_v2_src', 'contact') }}

{{ config(materialized='table') }}

WITH source_contact AS (
    SELECT
        id,
        full_name,
        email,
        account_ref
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
)

SELECT
    id AS "Id",
    CASE
        WHEN TRIM(full_name) IS NULL OR TRIM(full_name) = '' THEN NULL
        WHEN POSITION(' ' IN TRIM(full_name)) = 0 THEN NULL
        ELSE LEFT(TRIM(full_name), LENGTH(TRIM(full_name)) - POSITION(' ' IN REVERSE(TRIM(full_name))))
    END AS "FirstName",
    CASE
        WHEN TRIM(full_name) IS NULL OR TRIM(full_name) = '' THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(full_name)) = 0 THEN TRIM(full_name)
        ELSE RIGHT(TRIM(full_name), POSITION(' ' IN REVERSE(TRIM(full_name))) - 1)
    END AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_contact