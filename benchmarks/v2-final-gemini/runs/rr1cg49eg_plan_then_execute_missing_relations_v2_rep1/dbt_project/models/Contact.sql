{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    SPLIT_PART(contact.full_name, ' ', 1) AS "FirstName",
    COALESCE(SPLIT_PART(contact.full_name, ' ', 2), contact.full_name, 'Unknown') AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    acc.id AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    contact.account_ref = acc.id
