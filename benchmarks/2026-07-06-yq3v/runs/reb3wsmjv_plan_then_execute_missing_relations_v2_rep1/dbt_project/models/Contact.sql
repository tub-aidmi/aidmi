{{ config(materialized='table') }}

SELECT
    TRIM(contact.id) AS "Id",
    NULLIF(TRIM(SPLIT_PART(TRIM(contact.full_name), ' ', 1)), '') AS "FirstName",
    COALESCE(
        TRIM(SUBSTRING(TRIM(contact.full_name) FROM POSITION(' ' IN TRIM(contact.full_name)) + 1)),
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    TRIM(contact.account_ref) AS "AccountId",
    TRIM(contact.id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact
