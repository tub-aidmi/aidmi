-- depends_on: {{ ref('account') }}
{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN c.full_name IS NOT NULL AND POSITION(' ' IN TRIM(c.full_name)) > 0 THEN SUBSTRING(TRIM(c.full_name) FROM 1 FOR POSITION(' ' IN TRIM(c.full_name)) - 1)
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN c.full_name IS NOT NULL AND POSITION(' ' IN TRIM(c.full_name)) > 0 THEN SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1)
            WHEN c.full_name IS NOT NULL THEN TRIM(c.full_name)
            ELSE NULL
        END,
        'Unknown' -- LastName is NOT NULL, so provide a fallback
    ) AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    NULL AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON c.account_ref = a.id