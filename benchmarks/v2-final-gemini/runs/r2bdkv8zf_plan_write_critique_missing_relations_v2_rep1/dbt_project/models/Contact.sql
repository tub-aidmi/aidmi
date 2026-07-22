{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    TRIM(SPLIT_PART(contact.full_name, ' ', 1)) AS "FirstName",
    COALESCE(TRIM(SPLIT_PART(contact.full_name, ' ', 2)), 'Unknown Last Name') AS "LastName",
    TRIM(LOWER(contact.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account."Id" AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact
LEFT JOIN
    {{ ref('Account') }} AS account
    ON contact.account_ref = account."Legacy_Customer_ID__c"
