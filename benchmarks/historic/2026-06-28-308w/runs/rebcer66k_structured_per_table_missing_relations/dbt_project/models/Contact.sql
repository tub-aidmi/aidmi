-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN c.full_name IS NULL THEN NULL
        WHEN POSITION(' ' IN c.full_name) > 0 THEN TRIM(SUBSTRING(c.full_name FROM 1 FOR POSITION(' ' IN c.full_name) - 1))
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN c.full_name IS NULL THEN NULL
        WHEN POSITION(' ' IN c.full_name) > 0 THEN TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
        ELSE TRIM(c.full_name)
    END AS "LastName",
    c.email AS "Email",
    CAST(NULL AS TEXT) AS "Phone",
    CAST(NULL AS TEXT) AS "Title",
    CAST(NULL AS TEXT) AS "Role__c",
    CAST(NULL AS TEXT) AS "Preferred_Language__c",
    c.account_ref AS "AccountId",
    CAST(NULL AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Contact') }} AS c