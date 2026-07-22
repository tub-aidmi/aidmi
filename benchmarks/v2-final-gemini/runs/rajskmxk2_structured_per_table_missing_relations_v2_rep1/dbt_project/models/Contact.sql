-- {{ config(materialized='table') }}

WITH normalized_contacts AS (
    SELECT
        id,
        email,
        account_ref,
        TRIM(REGEXP_REPLACE(full_name, '\s+', ' ', 'g')) AS normalized_full_name
    FROM
        {{ source('fixture_missing_relations_v2_src', 'contact') }}
)
SELECT
    contact.id AS "Id",
    CASE
        WHEN POSITION(' ' IN REVERSE(normalized_contacts.normalized_full_name)) > 0
        THEN SUBSTRING(normalized_contacts.normalized_full_name, 1, LENGTH(normalized_contacts.normalized_full_name) - POSITION(' ' IN REVERSE(normalized_contacts.normalized_full_name)))
        ELSE NULL
    END AS "FirstName",
    COALESCE(
        CASE
            WHEN POSITION(' ' IN REVERSE(normalized_contacts.normalized_full_name)) > 0
            THEN SUBSTRING(normalized_contacts.normalized_full_name, LENGTH(normalized_contacts.normalized_full_name) - POSITION(' ' IN REVERSE(normalized_contacts.normalized_full_name)) + 2)
            WHEN normalized_contacts.normalized_full_name IS NOT NULL AND normalized_contacts.normalized_full_name <> ''
            THEN normalized_contacts.normalized_full_name
            ELSE NULL
        END,
        'Unknown'
    ) AS "LastName",
    contact.email AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    NULL::text AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    account.id AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    normalized_contacts AS contact
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON contact.account_ref = account.id