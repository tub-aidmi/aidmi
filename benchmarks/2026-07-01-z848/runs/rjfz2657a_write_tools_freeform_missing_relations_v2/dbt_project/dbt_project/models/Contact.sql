{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN POSITION(' ' IN full_name) > 0 THEN TRIM(SUBSTRING(full_name FROM 1 FOR POSITION(' ' IN full_name) - 1))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN POSITION(' ' IN full_name) > 0 THEN TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1))
            ELSE full_name
        END,
        'Unknown Contact'
    ) AS "LastName",
    email AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    NULL::text AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }}
