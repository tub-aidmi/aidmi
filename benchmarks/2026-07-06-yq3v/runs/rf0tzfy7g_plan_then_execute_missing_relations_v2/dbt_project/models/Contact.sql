{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    COALESCE(
        SPLIT_PART(c.full_name, ' ', 2),
        SPLIT_PART(c.full_name, ' ', 1),
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON c.account_ref = a.id
