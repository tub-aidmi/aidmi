-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    SPLIT_PART(TRIM(full_name), ' ', 1) AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SPLIT_PART(TRIM(full_name), ' ', 2)), ''), -- Second word, if it exists and is not empty
        TRIM(SPLIT_PART(TRIM(full_name), ' ', 1)),            -- Fallback to first word if no second word
        'Unknown'                                             -- Final fallback if full_name is entirely empty or null
    ) AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c", -- No mapping for enum values specified
    NULL AS "Preferred_Language__c", -- No mapping for enum values specified
    account_ref AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
