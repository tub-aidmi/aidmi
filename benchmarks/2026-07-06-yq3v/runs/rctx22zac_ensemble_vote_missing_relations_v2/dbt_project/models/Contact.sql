-- This dbt model transforms source contact data into the target Contact schema.

{{ config(materialized='table') }}

SELECT
    src_contact.id AS "Id",
    TRIM(SPLIT_PART(src_contact.full_name, ' ', 1)) AS "FirstName",
    COALESCE(
        NULLIF(TRIM(SPLIT_PART(src_contact.full_name, ' ', 2)), ''),
        NULLIF(TRIM(SPLIT_PART(src_contact.full_name, ' ', 1)), ''),
        'Unknown'
    ) AS "LastName",
    src_contact.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    src_account.id AS "AccountId",
    src_contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS src_contact
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS src_account
    ON src_contact.account_ref = src_account.id