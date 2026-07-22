{{
    config(materialized='table')
}}

WITH source_data AS (
    SELECT
        id,
        full_name,
        email,
        account_ref
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }}
)

SELECT
    id AS "Id",
    TRIM(REGEXP_REPLACE(full_name, '\s+\S+$', '')) AS "FirstName",
    COALESCE(
        TRIM(SUBSTRING(full_name FROM '\s+(\S+)$')),
        TRIM(full_name),
        'Unknown'
    ) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c", -- No source for this enum
    NULL AS "Preferred_Language__c", -- No source for this enum
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_data