{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN full_name IS NOT NULL AND POSITION(' ' IN full_name) > 0 THEN
            TRIM(LEFT(full_name, LENGTH(full_name) - LENGTH(SPLIT_PART(REVERSE(full_name), ' ', 1)) - 1))
        ELSE COALESCE(full_name, '')
    END AS "FirstName",
    CASE
        WHEN full_name IS NOT NULL AND POSITION(' ' IN full_name) > 0 THEN
            REVERSE(TRIM(SPLIT_PART(REVERSE(full_name), ' ', 1)))
        ELSE COALESCE(full_name, '')
    END AS "LastName",
    email AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}