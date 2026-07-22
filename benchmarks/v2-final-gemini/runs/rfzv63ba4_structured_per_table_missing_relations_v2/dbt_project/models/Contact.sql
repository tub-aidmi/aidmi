{{ config(materialized='table') }}

WITH normalize_full_name AS (
    SELECT
        id,
        email,
        account_ref,
        TRIM(full_name) AS trimmed_full_name
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
parsed_names AS (
    SELECT
        id,
        email,
        account_ref,
        CASE
            WHEN trimmed_full_name IS NULL OR trimmed_full_name = '' THEN NULL
            WHEN POSITION(' ' IN trimmed_full_name) > 0 THEN TRIM(SUBSTRING(trimmed_full_name FROM 1 FOR POSITION(' ' IN trimmed_full_name) - 1))
            ELSE TRIM(trimmed_full_name) -- If no space, the whole name is considered the first name
        END AS first_name,
        CASE
            WHEN trimmed_full_name IS NULL OR trimmed_full_name = '' THEN 'Unknown'
            WHEN POSITION(' ' IN trimmed_full_name) > 0 THEN TRIM(SUBSTRING(trimmed_full_name FROM POSITION(' ' IN trimmed_full_name) + 1))
            ELSE 'Unknown' -- If no space, last name is Unknown per target spec
        END AS last_name
    FROM
        normalize_full_name
)

SELECT
    p.id AS "Id",
    p.first_name AS "FirstName",
    COALESCE(p.last_name, 'Unknown') AS "LastName", -- Ensure LastName is not NULL as it's NOT NULL in target
    TRIM(p.email) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    p.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    parsed_names AS p
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON p.account_ref = a.id
```