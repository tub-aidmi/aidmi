{{ config(materialized='table') }}

SELECT
    CAST(id AS text) AS "Id",
    CASE
        WHEN POSITION(' ' IN TRIM(full_name)) > 0
        THEN TRIM(SPLIT_PART(full_name, ' ', 1))
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN POSITION(' ' IN TRIM(full_name)) > 0
        THEN TRIM(SUBSTRING(TRIM(full_name) FROM POSITION(' ' IN TRIM(full_name)) + 1))
        ELSE TRIM(full_name)
    END AS "LastName",
    CAST(email AS text) AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    NULL::text AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    a.id AS "AccountId",
    CAST(id AS text) AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c.account_ref = a.id