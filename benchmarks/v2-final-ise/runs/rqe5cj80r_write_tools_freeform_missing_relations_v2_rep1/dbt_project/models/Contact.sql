{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN c.full_name ~ '\s' THEN
            TRIM(SPLIT_PART(c.full_name, ' ', 1))
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN c.full_name ~ '\s' THEN
            TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
        ELSE TRIM(c.full_name)
    END AS "LastName",
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
    {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c.account_ref = a.id
