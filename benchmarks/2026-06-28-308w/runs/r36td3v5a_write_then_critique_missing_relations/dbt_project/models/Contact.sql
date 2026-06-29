
{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    NULLIF(TRIM(SPLIT_PART(contact.full_name, ' ', 1)), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(SPLIT_PART(contact.full_name, ' ', 2)), ''), 'Unknown') AS "LastName",
    contact.email AS "Email",
    CAST(NULL AS TEXT) AS "Phone",
    CAST(NULL AS TEXT) AS "Title",
    CAST(NULL AS TEXT) AS "Role__c",
    CAST(NULL AS TEXT) AS "Preferred_Language__c",
    contact.account_ref AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Contact') }} AS contact
