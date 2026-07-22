-- depends_on: {{ source('fixture_missing_relations_v2_src', 'contact') }}

{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    CASE
        WHEN contact.full_name IS NULL THEN NULL
        WHEN POSITION(' ' IN TRIM(contact.full_name)) > 0 THEN SPLIT_PART(TRIM(contact.full_name), ' ', 1)
        ELSE NULL -- If only one word, consider it part of Last Name as Last Name is NOT NULL
    END AS "FirstName",
    CASE
        WHEN contact.full_name IS NULL THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(contact.full_name)) > 0 THEN SUBSTRING(TRIM(contact.full_name) FROM POSITION(' ' IN TRIM(contact.full_name)) + 1)
        ELSE TRIM(contact.full_name) -- If only one word, it's the Last Name
    END AS "LastName",
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