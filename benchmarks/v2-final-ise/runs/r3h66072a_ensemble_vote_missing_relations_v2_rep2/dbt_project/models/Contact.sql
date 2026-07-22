{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    TRIM(SPLIT_PART(full_name, ' ', 1)) AS "FirstName",
    COALESCE(TRIM(SPLIT_PART(full_name, ' ', 2)), '') AS "LastName",
    CAST(email AS TEXT) AS "Email",
    CAST(NULL AS TEXT) AS "Phone",
    CAST(NULL AS TEXT) AS "Title",
    CAST(NULL AS TEXT) AS "Role__c",
    CAST(NULL AS TEXT) AS "Preferred_Language__c",
    CAST(account_ref AS TEXT) AS "AccountId",
    CAST(id AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}