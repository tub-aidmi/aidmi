{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN TRIM(full_name) IS NULL OR TRIM(full_name) = '' THEN NULL
        WHEN POSITION(' ' IN TRIM(full_name)) > 0 THEN TRIM(SUBSTRING(TRIM(full_name) FROM 1 FOR POSITION(' ' IN TRIM(full_name)) - 1))
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN TRIM(full_name) IS NULL OR TRIM(full_name) = '' THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(full_name)) > 0 THEN TRIM(SUBSTRING(TRIM(full_name) FROM POSITION(' ' IN TRIM(full_name)) + 1))
        ELSE COALESCE(TRIM(full_name), 'Unknown')
    END AS "LastName",
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
