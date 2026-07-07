-- models/Contact.sql
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN POSITION(' ' IN full_name) > 0 THEN TRIM(SUBSTRING(full_name FOR POSITION(' ' IN full_name) - 1))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1)),
        full_name,
        'Unknown'
    ) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c", -- No source for this enum
    NULL AS "Preferred_Language__c", -- No source for this enum
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }}
