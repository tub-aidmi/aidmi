-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    TRIM(SPLIT_PART(src.full_name, ' ', 1)) AS "FirstName",
    COALESCE(TRIM(SUBSTRING(src.full_name FROM POSITION(' ' IN src.full_name) + 1)), 'Unknown') AS "LastName",
    src.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    src.account_ref AS "AccountId",
    src.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS src