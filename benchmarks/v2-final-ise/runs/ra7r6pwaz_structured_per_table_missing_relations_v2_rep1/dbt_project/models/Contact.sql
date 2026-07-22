{{ config(materialized='table') }}

SELECT 
    CAST(id AS TEXT) AS "Id",
    CASE 
        WHEN POSITION(' ' IN COALESCE(full_name, '')) > 0 
        THEN TRIM(SPLIT_PART(full_name, ' ', 1))
        ELSE COALESCE(TRIM(full_name), '')
    END AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SPLIT_PART(COALESCE(full_name, ''), ' ', 2)), ''),
        COALESCE(TRIM(full_name), '')
    ) AS "LastName",
    CAST(email AS TEXT) AS "Email",
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