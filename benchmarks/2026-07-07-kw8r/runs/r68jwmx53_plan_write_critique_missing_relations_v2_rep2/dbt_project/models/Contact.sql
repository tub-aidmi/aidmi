{{ config(materialized='table') }}
SELECT
    c.id AS "Id",
    INITCAP(TRIM(SPLIT_PART(c.full_name, ' ', 1))) AS "FirstName",
    INITCAP(TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))) AS "LastName",
    LOWER(c.email) AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    NULL::text AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c.account_ref = a.id