{{ config(materialized='table') }}

WITH source_contact AS (
    SELECT
        id,
        full_name,
        email,
        account_ref
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
source_account AS (
    SELECT
        id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)

SELECT
    sc.id AS "Id",
    CASE
        WHEN sc.full_name IS NULL OR sc.full_name = '' THEN NULL
        WHEN POSITION(' ' IN sc.full_name) = 0 THEN NULL
        ELSE SUBSTRING(sc.full_name, 1, LENGTH(sc.full_name) - POSITION(' ' IN REVERSE(sc.full_name)) - 1)
    END AS "FirstName",
    CASE
        WHEN sc.full_name IS NULL OR sc.full_name = '' THEN '' -- LastName is NOT NULL, so use empty string if full_name is missing
        WHEN POSITION(' ' IN sc.full_name) = 0 THEN sc.full_name
        ELSE SUBSTRING(sc.full_name, LENGTH(sc.full_name) - POSITION(' ' IN REVERSE(sc.full_name)) + 2)
    END AS "LastName",
    sc.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    sa.id AS "AccountId",
    sc.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_contact sc
LEFT JOIN source_account sa ON sc.account_ref = sa.id
