
{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    CASE
        WHEN src.full_name IS NULL THEN NULL
        WHEN POSITION(' ' IN TRIM(src.full_name)) = 0 THEN NULL
        ELSE SPLIT_PART(TRIM(src.full_name), ' ', 1)
    END AS "FirstName",
    CASE
        WHEN src.full_name IS NULL THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(src.full_name)) = 0 THEN TRIM(src.full_name)
        ELSE SUBSTRING(TRIM(src.full_name) FROM POSITION(' ' IN TRIM(src.full_name)) + 1)
    END AS "LastName",
    src.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    src.account_ref AS "AccountId",
    src.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Contact') }} AS src
