{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(
        CASE
            WHEN POSITION(' ' IN TRIM(c.full_name)) > 0
            THEN SUBSTRING(TRIM(c.full_name), 1, POSITION(' ' IN TRIM(c.full_name)) - 1)
            ELSE TRIM(c.full_name)
        END
    ) AS "FirstName",
    COALESCE(
        TRIM(
            CASE
                WHEN POSITION(' ' IN TRIM(c.full_name)) > 0
                THEN SUBSTRING(TRIM(c.full_name), POSITION(' ' IN TRIM(c.full_name)) + 1)
                ELSE NULL -- Return NULL if no space, then COALESCE handles NOT NULL requirement
            END
        ), 'Unknown'
    ) AS "LastName",
    TRIM(LOWER(c.email)) AS "Email",
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
    ON TRIM(c.account_ref) = TRIM(a.id)