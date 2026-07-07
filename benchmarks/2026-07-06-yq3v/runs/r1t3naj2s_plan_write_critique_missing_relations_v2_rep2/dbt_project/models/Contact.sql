-- depends_on: {{ source('fixture_missing_relations_v2_src', 'contact') }}

{{ config(materialized='table') }}

SELECT
    MD5(contact.id) AS "Id",
    TRIM(SPLIT_PART(contact.full_name, ' ', 1)) AS "FirstName",
    COALESCE(TRIM(SUBSTRING(contact.full_name FROM POSITION(' ' IN contact.full_name) + 1)), 'Unknown') AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    MD5(contact.account_ref) AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact