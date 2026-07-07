{{ config(materialized='table') }}

WITH contact_prep AS (
    SELECT
        c.id,
        c.email,
        c.account_ref,
        TRIM(c.full_name) AS full_name_trimmed,
        REGEXP_SPLIT_TO_ARRAY(TRIM(c.full_name), '\\s+') AS name_parts
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
)
SELECT
    cp.id AS "Id",
    CASE
        WHEN ARRAY_LENGTH(cp.name_parts, 1) > 1 THEN ARRAY_TO_STRING(cp.name_parts[1 : ARRAY_LENGTH(cp.name_parts, 1) - 1], ' ')
        ELSE NULL
    END AS "FirstName",
    -- LastName is NOT NULL, ensure it's always populated.
    -- If full_name has parts, the last part is LastName. If only one part, that is LastName.
    cp.name_parts[ARRAY_LENGTH(cp.name_parts, 1)] AS "LastName",
    cp.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    cp.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    contact_prep AS cp
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    cp.account_ref = a.id
