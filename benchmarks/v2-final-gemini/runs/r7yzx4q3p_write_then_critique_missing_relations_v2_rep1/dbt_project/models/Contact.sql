-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    NULLIF(TRIM(SPLIT_PART(c.full_name, ' ', 1)), '') AS "FirstName",
    COALESCE(
        CASE
            WHEN TRIM(c.full_name) LIKE '% %' THEN
                NULLIF(TRIM(SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1)), '')
            ELSE
                NULL
        END,
        'Unknown'
    ) AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a."Id" AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ ref('Account') }} AS a
ON
    c.account_ref = a."Legacy_Customer_ID__c"