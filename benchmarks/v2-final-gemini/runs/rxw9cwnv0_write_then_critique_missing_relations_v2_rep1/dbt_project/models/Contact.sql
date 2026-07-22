-- dbt model for Contact
{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN TRIM(c.full_name) IS NULL OR TRIM(c.full_name) = '' THEN NULL
        WHEN POSITION(' ' IN REVERSE(TRIM(c.full_name))) = 0 THEN NULL
        ELSE SUBSTRING(TRIM(c.full_name) FROM 1 FOR LENGTH(TRIM(c.full_name)) - POSITION(' ' IN REVERSE(TRIM(c.full_name))))
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN TRIM(c.full_name) IS NULL OR TRIM(c.full_name) = '' THEN 'Unknown'
            WHEN POSITION(' ' IN REVERSE(TRIM(c.full_name))) = 0 THEN TRIM(c.full_name)
            ELSE SUBSTRING(TRIM(c.full_name) FROM LENGTH(TRIM(c.full_name)) - POSITION(' ' IN REVERSE(TRIM(c.full_name))) + 2)
        END,
    'Unknown') AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    acc.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON c.account_ref = acc.id