{{ config(materialized='table') }}

SELECT
    id AS "Id",
    TRIM(SPLIT_PART(full_name, ' ', 1)) AS "FirstName",
    COALESCE(TRIM(SPLIT_PART(full_name, ' ', 2)), 'Unknown') AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c", -- No source for enum mapping
    NULL AS "Preferred_Language__c", -- No source for enum mapping
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }}
