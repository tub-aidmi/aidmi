{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE
        WHEN c.full_name IS NOT NULL THEN
            TRIM(SPLIT_PART(c.full_name, ' ', 1))
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN c.full_name IS NOT NULL THEN
            TRIM(
                SUBSTRING(
                    c.full_name,
                    POSITION(' ' IN c.full_name) + 1
                )
            )
        ELSE 'Unknown'
    END AS "LastName",
    TRIM(c.email) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    COALESCE(
        (SELECT a.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a WHERE a.id = c.account_ref),
        (SELECT a.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a WHERE a.name = c.company_name)
    ) AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
