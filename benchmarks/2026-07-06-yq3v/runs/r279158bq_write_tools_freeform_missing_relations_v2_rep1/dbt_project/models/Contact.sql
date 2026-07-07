{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        id,
        full_name,
        email,
        account_ref,
        company_name
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }}
)

SELECT
    id AS "Id",
    CASE
        WHEN POSITION(' ' IN TRIM(full_name)) > 0 THEN SUBSTRING(TRIM(full_name) FROM 1 FOR POSITION(' ' IN TRIM(full_name)) - 1)
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN POSITION(' ' IN TRIM(full_name)) > 0 THEN SUBSTRING(TRIM(full_name) FROM POSITION(' ' IN TRIM(full_name)) + 1)
            ELSE TRIM(full_name)
        END,
        'Unknown Contact'
    ) AS "LastName",
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
FROM
    source_data