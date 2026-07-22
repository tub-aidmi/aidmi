{{ config(materialized='table') }}

WITH source_contact AS (
    SELECT
        id,
        full_name,
        email,
        account_ref
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
contact_parsed_name AS (
    SELECT
        id,
        email,
        account_ref,
        TRIM(full_name) AS trimmed_full_name,
        CASE
            WHEN TRIM(full_name) IS NOT NULL AND POSITION(' ' IN TRIM(full_name)) > 0
            THEN TRIM(SUBSTRING(TRIM(full_name) FOR POSITION(' ' IN TRIM(full_name)) - 1))
            ELSE NULL
        END AS first_name_raw,
        CASE
            WHEN TRIM(full_name) IS NOT NULL AND POSITION(' ' IN TRIM(full_name)) > 0
            THEN TRIM(SUBSTRING(TRIM(full_name) FROM POSITION(' ' IN TRIM(full_name)) + 1))
            WHEN TRIM(full_name) IS NOT NULL AND TRIM(full_name) != ''
            THEN TRIM(full_name)
            ELSE NULL
        END AS last_name_raw
    FROM source_contact
)
SELECT
    cpn.id AS "Id",
    cpn.first_name_raw AS "FirstName",
    COALESCE(cpn.last_name_raw, 'Unknown') AS "LastName",
    cpn.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    cpn.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    contact_parsed_name AS cpn
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    cpn.account_ref = a.id