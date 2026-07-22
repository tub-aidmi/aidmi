
{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(SPLIT_PART(TRIM(c.full_name), ' ', 1)) AS "FirstName",
    COALESCE(TRIM(SPLIT_PART(TRIM(c.full_name), ' ', -1)), 'Unknown') AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    c.account_ref AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_src', 'Account') }} AS a
ON
    c.account_ref = a.id
