-- noinspection SpellCheckingInspection
{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN POSITION(' ' IN TRIM(c.full_name)) > 0 THEN SPLIT_PART(TRIM(c.full_name), ' ', 1)
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1)), ''),
        TRIM(c.full_name),
        'Unknown' -- Fallback for NOT NULL LastName if full_name is entirely missing/empty
    ) AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c", -- No direct source, defaulting to NULL
    NULL AS "Preferred_Language__c", -- No direct source, defaulting to NULL
    c.account_ref AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c