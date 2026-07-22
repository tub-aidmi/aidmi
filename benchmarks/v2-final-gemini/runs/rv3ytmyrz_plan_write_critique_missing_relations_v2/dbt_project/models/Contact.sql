{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN TRIM(c.full_name) IS NULL OR TRIM(c.full_name) = '' THEN NULL
        ELSE (regexp_split_to_array(TRIM(c.full_name), E'\\s+'))[1]
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN TRIM(c.full_name) IS NULL OR TRIM(c.full_name) = '' THEN NULL
            ELSE (regexp_split_to_array(TRIM(c.full_name), E'\\s+'))[array_length(regexp_split_to_array(TRIM(c.full_name), E'\\s+'), 1)]
        END,
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
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
