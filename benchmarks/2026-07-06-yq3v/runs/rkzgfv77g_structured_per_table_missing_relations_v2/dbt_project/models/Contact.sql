-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlDialectInspection

{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    CASE
        WHEN POSITION(' ' IN contact.full_name) > 0 THEN LEFT(contact.full_name, POSITION(' ' IN contact.full_name) - 1)
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN POSITION(' ' IN contact.full_name) > 0 THEN SUBSTRING(contact.full_name FROM POSITION(' ' IN contact.full_name) + 1)
        ELSE contact.full_name -- Ensures LastName is not NULL if full_name exists
    END AS "LastName",
    contact.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c", -- No source mapping, default to NULL
    NULL AS "Preferred_Language__c", -- No source mapping, default to NULL
    contact.account_ref AS "AccountId", -- Assuming account_ref directly maps to Account Id
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact