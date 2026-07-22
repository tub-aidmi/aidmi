{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN POSITION(' ' IN REVERSE(full_name)) > 0 THEN LEFT(full_name, LENGTH(full_name) - POSITION(' ' IN REVERSE(full_name)))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN POSITION(' ' IN REVERSE(full_name)) > 0 THEN RIGHT(full_name, POSITION(' ' IN REVERSE(full_name)) - 1)
            ELSE full_name
        END,
        'Unknown'
    ) AS "LastName", -- LastName is NOT NULL
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_ref AS "AccountId",
    NULL AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Contact') }}
