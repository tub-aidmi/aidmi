{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    CASE
        WHEN contact.full_name IS NULL THEN NULL
        ELSE SPLIT_PART(contact.full_name, ' ', 1)
    END AS "FirstName",
    COALESCE(SPLIT_PART(contact.full_name, ' ', -1), 'Unknown') AS "LastName",
    contact.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    CAST(NULL AS TEXT) AS "Role__c",
    CAST(NULL AS TEXT) AS "Preferred_Language__c",
    account.id AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS contact
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON contact.account_ref = account.id
