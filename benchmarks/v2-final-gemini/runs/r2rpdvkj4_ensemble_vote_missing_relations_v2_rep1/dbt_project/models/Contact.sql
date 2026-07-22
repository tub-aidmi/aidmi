{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN TRIM(c.full_name) IS NOT NULL AND TRIM(c.full_name) != ''
        THEN (REGEXP_MATCH(TRIM(c.full_name), '^(.*?)(?:\s+(.*))?$'))[1]
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN TRIM(c.full_name) IS NOT NULL AND TRIM(c.full_name) != ''
            THEN (REGEXP_MATCH(TRIM(c.full_name), '^(.*?)(?:\s+(.*))?$'))[2]
            ELSE NULL
        END, '') AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    c.account_ref = a.id
