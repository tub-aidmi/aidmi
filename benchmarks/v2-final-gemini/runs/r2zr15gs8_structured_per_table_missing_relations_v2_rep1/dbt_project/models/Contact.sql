-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(
        CASE
            WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN NULL
            WHEN POSITION(' ' IN TRIM(c.full_name)) > 0 THEN SUBSTRING(TRIM(c.full_name) FROM 1 FOR POSITION(' ' IN TRIM(c.full_name)) - 1)
            ELSE TRIM(c.full_name) -- Single word name, use as first name
        END
    ) AS "FirstName",
    COALESCE(
        TRIM(
            CASE
                WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN NULL
                WHEN POSITION(' ' IN TRIM(c.full_name)) > 0 THEN SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1)
                ELSE NULL -- Single word name, no last name
            END
        ),
        'Unknown' -- Default for NOT NULL LastName
    ) AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON c.account_ref = a.id