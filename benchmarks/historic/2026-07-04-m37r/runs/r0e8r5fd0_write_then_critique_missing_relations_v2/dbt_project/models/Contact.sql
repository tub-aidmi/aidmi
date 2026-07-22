-- noinspection SqlNoDataSourceInspection
-- noinspection SqlResolve

{{ config(materialized='table') }}

WITH cleaned_contacts AS (
    SELECT
        id AS contact_id,
        full_name,
        email,
        account_ref,
        company_name,
        TRIM(full_name) AS trimmed_full_name
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }}
)
SELECT
    contact.contact_id AS "Id",
    CASE
        WHEN contact.trimmed_full_name IS NULL OR contact.trimmed_full_name = '' THEN NULL
        WHEN POSITION(' ' IN contact.trimmed_full_name) > 0 THEN SUBSTRING(contact.trimmed_full_name FROM 1 FOR POSITION(' ' IN contact.trimmed_full_name) - 1)
        ELSE contact.trimmed_full_name -- If only one word, treat it as the first name if no last name is available
    END AS "FirstName",
    CASE
        WHEN contact.trimmed_full_name IS NULL OR contact.trimmed_full_name = '' THEN '' -- LastName is NOT NULL
        WHEN POSITION(' ' IN contact.trimmed_full_name) > 0 THEN SUBSTRING(contact.trimmed_full_name FROM POSITION(' ' IN contact.trimmed_full_name) + 1)
        ELSE '' -- If only one word, and it was taken as FirstName, LastName is empty to satisfy NOT NULL
    END AS "LastName",
    contact.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account.id AS "AccountId",
    contact.contact_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_contacts AS contact
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON contact.account_ref = account.id