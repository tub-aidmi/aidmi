{{ config(materialized='table') }}

SELECT
    TRIM(c.id) AS "Id",
    INITCAP(TRIM(SPLIT_PART(c.full_name, ' ', 1))) AS "FirstName",
    COALESCE(
        INITCAP(TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))),
        INITCAP(TRIM(c.full_name)),
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    TRIM(a.id) AS "AccountId",
    TRIM(c.id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON TRIM(c.account_ref) = TRIM(a.id)