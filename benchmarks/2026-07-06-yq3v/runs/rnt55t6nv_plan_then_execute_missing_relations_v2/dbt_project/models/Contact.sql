-- This dbt model transforms raw contact data into the target Contact schema.
-- It joins with the account table to resolve the AccountId and handles name splitting.

{{ config(materialized='table') }}

SELECT
    TRIM(c.id) AS "Id",
    CASE
        WHEN POSITION(' ' IN TRIM(c.full_name)) > 0 THEN TRIM(SPLIT_PART(TRIM(c.full_name), ' ', 1))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        TRIM(SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1)),
        TRIM(c.full_name),
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    TRIM(c.id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON TRIM(c.account_ref) = TRIM(a.id)