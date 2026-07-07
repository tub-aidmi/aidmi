{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    CASE
        WHEN POSITION(' ' IN contact.full_name) > 0
        THEN SUBSTRING(contact.full_name FROM 1 FOR POSITION(' ' IN contact.full_name) - 1)
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SUBSTRING(contact.full_name FROM POSITION(' ' IN contact.full_name) + 1)), ''),
        NULLIF(TRIM(contact.full_name), ''),
        'Unknown'
    ) AS "LastName",
    contact.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    contact.account_ref AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact
