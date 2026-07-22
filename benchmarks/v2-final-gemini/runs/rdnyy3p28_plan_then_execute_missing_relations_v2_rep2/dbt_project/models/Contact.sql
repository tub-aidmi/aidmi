{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    CASE
        WHEN contact.full_name IS NULL OR TRIM(contact.full_name) = '' THEN NULL
        ELSE SPLIT_PART(TRIM(contact.full_name), ' ', 1)
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN contact.full_name IS NULL OR TRIM(contact.full_name) = '' THEN NULL
            WHEN STRPOS(TRIM(contact.full_name), ' ') = 0 THEN NULL -- No space, implies single word, so no last name
            ELSE SPLIT_PART(TRIM(contact.full_name), ' ', -1)
        END,
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
