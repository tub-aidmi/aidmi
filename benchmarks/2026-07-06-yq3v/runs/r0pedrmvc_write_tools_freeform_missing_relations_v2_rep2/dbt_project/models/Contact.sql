{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN POSITION(' ' IN full_name) > 0
        THEN SUBSTRING(full_name FROM 1 FOR POSITION(' ' IN full_name) - 1)
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN POSITION(' ' IN full_name) > 0
            THEN SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1)
            ELSE full_name
        END, id) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }}