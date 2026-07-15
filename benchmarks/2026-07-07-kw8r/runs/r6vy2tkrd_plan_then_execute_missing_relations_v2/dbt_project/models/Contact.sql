{{ config(materialized='table') }}

SELECT
    id AS "Id",
    TRIM(SPLIT_PART(COALESCE(full_name, ''), ' ', 1)) AS "FirstName",
    COALESCE(
        NULLIF(
            TRIM(
                SUBSTRING(
                    COALESCE(full_name, '') 
                    FROM POSITION(' ' IN COALESCE(full_name, '')) + 1
                )
            ), 
            ''
        ), 
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    REGEXP_REPLACE(LOWER(TRIM(account_ref)), '^[^a-z0-9]*', '') AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}