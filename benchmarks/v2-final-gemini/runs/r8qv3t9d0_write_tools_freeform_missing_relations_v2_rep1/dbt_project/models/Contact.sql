{{ config(materialized='table') }}

SELECT
    id AS "Id",
    NULLIF(TRIM(SUBSTRING(full_name FROM 1 FOR POSITION(' ' IN full_name) - 1)), '') AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1)), ''),
        full_name,
        'Unknown'
    ) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_ref AS "AccountId", -- Assuming account_ref maps directly to Account.Id
    id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
