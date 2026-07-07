{{ config(materialized='table') }}

SELECT
    id AS "Id",
    CASE
        WHEN full_name IS NOT NULL AND POSITION(' ' IN full_name) > 0
            THEN TRIM(SPLIT_PART(full_name, ' ', 1))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN full_name IS NOT NULL AND POSITION(' ' IN full_name) > 0
                THEN TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1))
            WHEN full_name IS NOT NULL AND TRIM(full_name) != ''
                THEN TRIM(full_name)
            ELSE NULL
        END,
        'Unknown'
    ) AS "LastName",
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
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}