-- depends_on: {{ ref('account') }}

{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN c.full_name IS NOT NULL AND POSITION(' ' IN c.full_name) > 0
        THEN TRIM(SUBSTRING(c.full_name FROM 1 FOR POSITION(' ' IN c.full_name) - 1))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN c.full_name IS NULL THEN 'Unknown'
            WHEN POSITION(' ' IN c.full_name) = 0 THEN TRIM(c.full_name)
            ELSE TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
        END,
        'Unknown' -- Final fallback for NOT NULL LastName
    ) AS "LastName",
    c.email AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    NULL::text AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    c.account_ref = a.id