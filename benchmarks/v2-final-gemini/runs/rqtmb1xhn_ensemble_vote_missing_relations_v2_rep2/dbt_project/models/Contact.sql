{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(SPLIT_PART(COALESCE(c.full_name, ''), ' ', 1)) AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SUBSTRING(COALESCE(c.full_name, '') FROM POSITION(' ' IN COALESCE(c.full_name, '')) + 1)), ''),
        TRIM(COALESCE(c.full_name, ''))
    ) AS "LastName",
    c.email AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
ON
    c.account_ref = a.id
