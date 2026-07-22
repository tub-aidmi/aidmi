{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    CASE
        WHEN POSITION(' ' IN TRIM(contact.full_name)) > 0
        THEN TRIM(SUBSTRING(TRIM(contact.full_name) FOR POSITION(' ' IN TRIM(contact.full_name)) - 1))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        NULLIF(
            CASE
                WHEN POSITION(' ' IN TRIM(contact.full_name)) > 0
                THEN TRIM(SUBSTRING(TRIM(contact.full_name) FROM POSITION(' ' IN TRIM(contact.full_name)) + 1))
                ELSE TRIM(contact.full_name)
            END, ''
        ), 'Unknown'
    ) AS "LastName",
    TRIM(contact.email) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account.id AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON TRIM(contact.account_ref) = TRIM(account.id)'''