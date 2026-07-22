
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    SPLIT_PART(full_name, ' ', 1) AS "FirstName",
    COALESCE(
        TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1)),
        full_name,
        'Unknown Last Name'
    ) AS "LastName",
    email AS "Email",
    CAST(NULL AS TEXT) AS "Phone",
    CAST(NULL AS TEXT) AS "Title",
    CAST(NULL AS TEXT) AS "Role__c",
    CAST(NULL AS TEXT) AS "Preferred_Language__c",
    account_ref AS "AccountId",
    CAST(NULL AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Contact') }}
