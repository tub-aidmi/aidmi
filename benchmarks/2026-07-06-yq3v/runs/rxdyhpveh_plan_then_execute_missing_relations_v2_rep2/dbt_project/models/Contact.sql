-- noinspection SqlNoDataSourceInspection
-- noinspection SqlDialectInspection

{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    TRIM(SPLIT_PART(contact.full_name, ' ', 1)) AS "FirstName",
    COALESCE(
        TRIM(SUBSTRING(contact.full_name FROM POSITION(' ' IN contact.full_name) + 1)),
        TRIM(contact.full_name),
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(contact.email)) AS "Email",
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
    ON contact.account_ref = account.id